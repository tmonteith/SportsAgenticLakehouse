package poller

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaPublisher produces to one topic over plaintext (pure Go, no cgo).
type KafkaPublisher struct {
	w       *kafka.Writer
	timeout time.Duration
}

// NewKafkaPublisher returns a synchronous producer for topic on the comma-separated
// bootstrap brokers. It never creates topics: cfb.live_games is a KafkaTopic resource.
func NewKafkaPublisher(bootstrap, topic string) *KafkaPublisher {
	var brokers []string
	for _, b := range strings.Split(bootstrap, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return &KafkaPublisher{
		w: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{}, // key game_id; the topic has one partition
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: false,
			BatchSize:              1000,
			BatchTimeout:           10 * time.Millisecond,
			WriteTimeout:           10 * time.Second,
			ReadTimeout:            10 * time.Second,
			MaxAttempts:            3,
		},
		timeout: 30 * time.Second,
	}
}

// Publish writes msgs and waits for the broker's acknowledgement.
func (k *KafkaPublisher) Publish(ctx context.Context, msgs []Message) error {
	ctx, cancel := context.WithTimeout(ctx, k.timeout)
	defer cancel()
	out := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		out[i] = kafka.Message{Key: m.Key, Value: m.Value}
	}
	return k.w.WriteMessages(ctx, out...)
}

// Close flushes and closes the connection.
func (k *KafkaPublisher) Close() error { return k.w.Close() }
