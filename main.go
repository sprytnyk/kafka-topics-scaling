package main

import (
	"context"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})
}

func createTopic(admin *kafka.AdminClient, topic string, numPartitions int, replicationFactor int) {
	// Fetch metadata for all topics to avoid auto-creation
	metadata, err := admin.GetMetadata(nil, true, 10_000)
	if err != nil {
		log.WithError(err).Error("Failed to fetch metadata")
		return
	}

	if _, exists := metadata.Topics[topic]; exists {
		log.Infof("Topic '%s' already exists. Skipping creation.", topic)
		return
	}

	// Create topic explicitly
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		}},
	)
	if err != nil {
		log.WithError(err).Error("Failed to create topic")
		return
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			log.WithFields(log.Fields{
				"topic": result.Topic,
				"error": result.Error,
			}).Error("Topic creation failed")
		} else {
			log.Infof("Topic '%s' created successfully", result.Topic)
		}
	}
}

// rebalanceCallback handles partition assignment and revocation events
func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch e := event.(type) {
	case kafka.AssignedPartitions:
		log.WithField("partitions", e.Partitions).Info("Partitions assigned.")
		return c.Assign(e.Partitions)
	case kafka.RevokedPartitions:
		log.WithField("partitions", e.Partitions).Info("Partitions revoked.")
		return c.Unassign()
	default:
		log.Infof("Unknown rebalance event: %v", e)
		return nil
	}
}

// runConsumer starts a Kafka consumer that polls messages
func runConsumer(ctx context.Context, wg *sync.WaitGroup, config *kafka.ConfigMap, topics []string, i int) {
	defer wg.Done()

	c, err := kafka.NewConsumer(config)
	if err != nil {
		log.WithError(err).Error("Failed to create consumer")
		return
	}
	defer c.Close()

	if err := c.SubscribeTopics(topics, rebalanceCallback); err != nil {
		log.WithError(err).Error("Failed to subscribe to topics")
		return
	}

	log.Infof("Consumer %d is polling for messages...", i)

	for {
		select {
		case <-ctx.Done():
			log.Infof("Consumer %d exiting...", i)
			return
		default:
			ev := c.Poll(2000)
			if msg, ok := ev.(*kafka.Message); ok {
				offsets, err := c.CommitMessage(msg)
				currentOffset := int64(msg.TopicPartition.Offset)
				commitOffset := int64(0)
				if len(offsets) > 0 {
					commitOffset = int64(offsets[0].Offset)
				}
				if !(commitOffset-1 == currentOffset) {
					log.WithFields(log.Fields{
						"currentOffset": currentOffset,
						"commitOffset":  commitOffset,
						"offsets":       offsets,
						"msg":           msg,
					}).Error("Kafka offset mismatch.")
				}

				if err != nil {
					log.Errorf("Fault commit consumer: (offsets: %v), {%v}", offsets, err)
					return
				} else {
					log.WithFields(log.Fields{
						"currentOffset": currentOffset,
						"commitOffset":  commitOffset,
						"offsets":       offsets,
						"msg":           msg,
					}).Info("Kafka offset update.")
				}
			}
		}
	}
}

// initConsumers starts multiple consumers concurrently
func initConsumers(ctx context.Context, wg *sync.WaitGroup, config *kafka.ConfigMap) {
	consumers := 2
	for i := range consumers {
		wg.Add(1)
		go runConsumer(ctx, wg, config, []string{"dummy"}, i)
	}
}

func main() {
	adminCfg := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
	}

	// Create AdminClient and topic
	admin, err := kafka.NewAdminClient(adminCfg)
	if err != nil {
		log.WithError(err).Fatal("Failed to create AdminClient")
	}
	defer admin.Close()

	createTopic(admin, "dummy", 10, 1)

	// Consumer config
	consumerCfg := &kafka.ConfigMap{
		"bootstrap.servers":             "localhost:29092",
		"group.id":                      "go-poc",
		"auto.offset.reset":             "earliest",
		"partition.assignment.strategy": "roundrobin",
		"enable.auto.commit":            false,   // disable auto commits
		"connections.max.idle.ms":       1800000, // 30 minutes
		"heartbeat.interval.ms":         10000,   // every 10s
		"session.timeout.ms":            90000,   // 1.5 minutes
		"socket.keepalive.enable":       true,    // sends TCP keepalive packets
		"metadata.max.age.ms":           180000,  // forces periodic metadata requests every 3 minutes

	}

	var wg sync.WaitGroup
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go initConsumers(ctx, &wg, consumerCfg)

	<-ctx.Done()
	log.Info("Shutdown signal received")

	wg.Wait()
	log.Info("All consumers shut down cleanly")
}
