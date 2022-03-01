package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":       "127.0.0.1",
		"acks":                    "1",
		"socket.keepalive.enable": true,
	})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// mensagem a ser mockada, deve ser em slice de byte
	j := []byte("{\"voting_id\": 1, \"state\": \"am\", \"total\": 100}")

	// topico do kafka a ser enviado a mensagem
	topic := "votometer-consolidated"

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          j,
	}, nil)

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
