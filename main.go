package main

import (
	"fmt"

	"kafkaGoClient/config"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	defer config.Close()

	c := config.C
	conf := config.KafkaConf

	assignPartition(c, conf.Topic, conf.StartOffset)
	readMessages(c, conf.EndOffset)
}

// 分配partition并定位offset
func assignPartition(c *kafka.Consumer, topic string, offset int64) {
	tp := kafka.TopicPartition{
		Topic:     &topic,
		Partition: 0,
		Offset:    kafka.Offset(offset),
	}

	if err := c.Assign([]kafka.TopicPartition{tp}); err != nil {
		panic(fmt.Sprintf("Assign failed: %v", err))
	}

	fmt.Printf("Assigned partition 0 at offset %d\n", offset)
}

// 读取消息至endOffset
func readMessages(c *kafka.Consumer, endOffset int64) {
	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Read done: %v\n", err)
			break
		}

		currentOffset := int64(msg.TopicPartition.Offset)
		if currentOffset > endOffset {
			break
		}

		fmt.Println(string(msg.Value))
	}
}