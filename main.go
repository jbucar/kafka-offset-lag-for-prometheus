package main

import (
	"flag"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kouhin/envflag"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	activeOnly     = flag.Bool("active-only", false, "Show only consumers with an active consumer protocol.")
	kafkaBrokers   = flag.String("kafka-brokers", "localhost:9092", "Comma separated list of kafka brokers.")
	prometheusAddr = flag.String("prometheus-addr", ":7979", "Prometheus listen interface and port.")
	refreshInt     = flag.Int("refresh-interval", 15, "Time between offset refreshes in seconds.")
	saslUser       = flag.String("sasl-user", "", "SASL username.")
	saslPass       = flag.String("sasl-pass", "", "SASL password.")
	debug          = flag.Bool("debug", false, "Enable debug output.")
)

type TopicOffset struct {
	Begin int64
	End   int64
	Total int64
}
type TopicSet map[string]map[int32]TopicOffset
type ConsumerGroupTopics map[string]bool

func init() {
	if err := envflag.Parse(); err != nil {
		panic(err)
	}
	if *debug {
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	}
}

func main() {
	go func() {
		var cycle uint8
		config := sarama.NewConfig()
		config.ClientID = "kafka-offset-lag-for-prometheus"
		config.Version = sarama.V0_9_0_0
		if *saslUser != "" {
			config.Net.SASL.User = *saslUser
			config.Net.SASL.Password = *saslPass
		}
		client, err := sarama.NewClient(strings.Split(*kafkaBrokers, ","), config)

		if err != nil {
			log.Fatal("Unable to connect to given brokers.")
		}

		defer client.Close()

		for {
			topicSet := make(TopicSet)
			time.Sleep(time.Duration(*refreshInt) * time.Second)
			timer := prometheus.NewTimer(LookupHist)
			client.RefreshMetadata()
			// First grab our topics, all partiton IDs, and their offset head
			topics, err := client.Topics()
			if err != nil {
				log.Printf("Error fetching topics: %s", err.Error())
				continue
			}

			for _, topic := range topics {
				// Don't include internal topics
				if strings.HasPrefix(topic, "__") {
					continue
				}
				partitions, err := client.Partitions(topic)
				if err != nil {
					log.Printf("Error fetching partitions: %s", err.Error())
					continue
				}
				if *debug {
					log.Printf("Found topic '%s' with %d partitions", topic, len(partitions))
				}
				topicSet[topic] = make(map[int32]TopicOffset)

				var totalOffsets int64
				for _, partition := range partitions {
					oEnd, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
					if err != nil {
						log.Printf("Problem fetching offset for topic '%s', partition '%d'", topic, partition)
						continue
					}

					oBegin, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
					if err != nil {
						log.Printf("Problem fetching offset for topic '%s', partition '%d'", topic, partition)
						continue
					}

					var offs TopicOffset
					offs.Begin = oBegin
					offs.End = oEnd
					offs.Total = oEnd - oBegin
					topicSet[topic][partition] = offs

					totalOffsets += topicSet[topic][partition].Total
				}

				//	Calculate totalOffsets
				for _, partition := range partitions {
					var partitionPercentil float64
					if topicSet[topic][partition].Total > 0 {
						partitionPercentil = float64(topicSet[topic][partition].Total*100) / float64(totalOffsets)
					}

					OffsetDistribution.With(prometheus.Labels{
						"topic":     topic,
						"partition": strconv.FormatInt(int64(partition), 10),
					}).Set(partitionPercentil)
				}
			}

			// Prometheus SDK never TTLs out data points so tmp consumers last
			// forever. Ugly hack to clean up data points from time to time.
			if cycle >= 99 {
				OffsetLag.Reset()
				OffsetDistribution.Reset()
				cycle = 0
			}
			cycle++

			var wg sync.WaitGroup

			// Now lookup our group data using the metadata
			for _, broker := range client.Brokers() {
				// Sarama plays russian roulette with the brokers
				broker.Open(client.Config())
				_, err := broker.Connected()
				if err != nil {
					log.Printf("Could not speak to broker %s. Your advertised.listeners may be incorrect.", broker.Addr())
					continue
				}

				wg.Add(1)

				go func(broker *sarama.Broker) {
					defer wg.Done()
					refreshBroker(broker, topicSet)
				}(broker)
			}

			wg.Wait()
			timer.ObserveDuration()
		}
	}()
	prometheusListen(*prometheusAddr)
}

func refreshBroker(broker *sarama.Broker, topicSet TopicSet) {
	groupsRequest := new(sarama.ListGroupsRequest)
	groupsResponse, err := broker.ListGroups(groupsRequest)
	if err != nil {
		log.Printf("Could not list groups: %s\n", err.Error())
		return
	}

	//	Find all consumers
	describeGroupsRequest := new(sarama.DescribeGroupsRequest)
	for group, t := range groupsResponse.Groups {
		//log.Printf("Group: name=%s, type=%s\n", group, t)
		// we only want active consumers
		// do we want to filter by active consumers?
		if *activeOnly && t != "consumer" {
			continue
		}

		describeGroupsRequest.Groups = append(describeGroupsRequest.Groups, group)
	}

	//	Describe all consumers groups
	describeGroupsResponse, err := broker.DescribeGroups(describeGroupsRequest)
	if err != nil {
		log.Printf("Could not describe groups: %s\n", err.Error())
		return
	}

	//log.Printf("Groups: %d\n", len(describeGroupsResponse.Groups))
	for _, group := range describeGroupsResponse.Groups {
		consumerGroupTopics := make(ConsumerGroupTopics)

		//	Collect topics name from all members
		for memberName, member := range group.Members {
			consumerGroupMemberMetadata, err := member.GetMemberMetadata()
			if err != nil {
				log.Printf("Could not get metadata from %s: %s\n", memberName, err.Error())
				return
			}

			for _, topic := range consumerGroupMemberMetadata.Topics {
				consumerGroupTopics[topic] = true
			}
		}

		//	Fetch offset for all consumer topic
		for topic := range consumerGroupTopics {
			//log.Printf("Process group: groupId=%s, topic=%s\n", group.GroupId, topic)
			data := topicSet[topic]
			offsetsRequest := new(sarama.OffsetFetchRequest)
			offsetsRequest.Version = 1
			offsetsRequest.ConsumerGroup = group.GroupId
			for partition := range data {
				offsetsRequest.AddPartition(topic, partition)
			}

			offsetsResponse, err := broker.FetchOffset(offsetsRequest)
			if err != nil {
				log.Printf("Could not get offset: %s\n", err.Error())
			}

			for _, blocks := range offsetsResponse.Blocks {
				for partition, block := range blocks {
					// Because our offset operations aren't atomic we could end up with a negative lag
					var lag float64
					if block.Offset < 0 {
						lag = float64(data[partition].End - data[partition].Begin)
					} else {
						lag = float64(data[partition].End) - math.Max(float64(block.Offset), 0)
					}
					lag = math.Max(float64(lag), 0)

					if *debug {
						log.Printf("Discovered; group=%s, topic=%s, partition=%d offsets=(begin=%d,end=%d,total=%d,group=%d) -> lag=%f\n",
							group.GroupId, topic, partition, data[partition].Begin, data[partition].End, data[partition].Total, block.Offset, lag)
					}
					OffsetLag.With(prometheus.Labels{
						"topic": topic, "group": group.GroupId,
						"partition": strconv.FormatInt(int64(partition), 10),
					}).Set(lag)
				}
			}
		}
	}
}
