package producer

import (
	"errors"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ProducerCli struct {
	Producer     *kafka.Producer
	FlushTimeout int
}

// ConfigProducer - configure kafka producer
func ConfigProducer(url string, timeout int) (*ProducerCli, error) {
	if url == "" {
		return &ProducerCli{}, errors.New("invalid Url")
	}

	config := kafka.ConfigMap{"bootstrap.servers": url}

	p, err := kafka.NewProducer(&config)
	if err != nil {
		return &ProducerCli{}, nil
	}

	if timeout < 0 {
		timeout = 5000 // ms
	}

	pc := &ProducerCli{
		Producer:     p,
		FlushTimeout: timeout,
	}

	go pc.watch()

	return pc, nil
}

// watch - Handle message delivery reports
func (pc ProducerCli) watch() {
	for e := range pc.Producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				fmt.Printf("Delivered message to topic %v partition %v\n", ev.TopicPartition.Topic, ev.TopicPartition.Partition)
			}
		case kafka.Error:
			fmt.Printf("Kafka error: %v\n", ev.Error())
			if ev.IsFatal() {
				log.Fatal("Fatal error.... bye")
			}
		}

	}
}

// Stop  - close the producer after flushing
func (pc ProducerCli) Stop() {
	if num := pc.Producer.Flush(pc.FlushTimeout); num > 0 {
		fmt.Println("Un-flushed events ", num)
	}

	pc.Producer.Close()
}

// Post - send message to topic
func (pc ProducerCli) Post(message []byte, key, topic string) error {
	kMessage := &kafka.Message{
		Value:          message,
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	}

	if key != "" {
		kMessage.Key = []byte(key)
	}

	if err := pc.Producer.Produce(kMessage, nil); err != nil {
		return err
	}

	return nil
}
