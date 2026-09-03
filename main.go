package main

import (
	"fmt"
	"time"

	"kafkaGoClient/config"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	defer config.Close()

	c := config.C
	conf := config.KafkaConf

	subscribe(c, conf.Topic)
	seekToOffset(c, conf.Topic, conf.StartOffset)
	endOffset := conf.EndOffset
	readMessages(c, endOffset)
}

// 订阅topic
func subscribe(c *kafka.Consumer, topic string) {
	err := c.Subscribe(topic, nil)
	if err != nil {
		panic(err)
	}

	for {
		ev := c.Poll(1000)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *kafka.AssignedPartitions:
			fmt.Println("Partitions assigned:", e.Partitions)
			return
		case *kafka.Error:
			panic(fmt.Sprintf("Consumer error: %v", e))
		}
	}
}

// 设置Offset
func seekToOffset(c *kafka.Consumer, topic string, offset int64) {
	tp := kafka.TopicPartition{
		Topic:     &topic,
		Partition: 0,
		Offset:    kafka.Offset(offset),
	}

	err := c.Seek(tp, -1)
	if err != nil {
		panic(fmt.Sprintf("Seek failed: %v", err))
	}

	fmt.Println("Seek done, start reading...")
}

// 读取消息至endOffset
func readMessages(c *kafka.Consumer, endOffset int64) {
	for {
		msg, err := c.ReadMessage(5 * time.Second)
		if err != nil {
			fmt.Printf("Read error or timeout: %v\n", err)
			break
		}

		currentOffset := int64(msg.TopicPartition.Offset)
		if currentOffset > endOffset {
			fmt.Println("Reached end offset, stop.")
			break
		}

		fmt.Println(string(msg.Value))
	}
}

