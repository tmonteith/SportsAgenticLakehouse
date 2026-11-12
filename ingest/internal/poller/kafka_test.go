package poller

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestKafkaPublisherConfig(t *testing.T) {
	k := NewKafkaPublisher("sa-kafka-kafka-bootstrap.data:9092, other:9092", Topic)
	defer k.Close()
	w := k.w
	if w.Topic != "cfb.live_games" || w.AllowAutoTopicCreation || w.RequiredAcks != kafka.RequireAll {
		t.Errorf("writer config: topic=%s autocreate=%v acks=%v", w.Topic, w.AllowAutoTopicCreation, w.RequiredAcks)
	}
	if got := w.Addr.String(); got != "sa-kafka-kafka-bootstrap.data:9092,other:9092" {
		t.Errorf("brokers = %s", got)
	}
	if _, ok := w.Balancer.(*kafka.Hash); !ok {
		t.Errorf("balancer = %T, want key hash", w.Balancer)
	}
}
