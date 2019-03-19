package fileproducer

import (
	"time"

	"github.com/Shopify/sarama"
)

var brokers = []string{"127.0.0.1:9092"}

// NewProducer returns a new producer object
func NewProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Idempotent = false
	config.Producer.Return.Successes = true
	config.Producer.Flush.MaxMessages = 15000
	config.Producer.Flush.Frequency = 100 * time.Millisecond
	producer, err := sarama.NewSyncProducer(brokers, config)

	return producer, err
}

// PrepareMessages prepares messages to be sent to Kafka broker
func PrepareMessages(topic string, messages []string) []*sarama.ProducerMessage {
	var msgs []*sarama.ProducerMessage
	for _, m := range messages {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: -1,
			Value:     sarama.StringEncoder(m),
		}
		msgs = append(msgs, msg)
	}

	return msgs
}
