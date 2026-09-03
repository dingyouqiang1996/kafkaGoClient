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

	low, high := initAndGetWatermark(c, conf.Topic)
	if !validateOffsetRange(conf, low, high) {
		return
	}

	assignPartition(c, conf.Topic, conf.StartOffset)
	readMessages(c, conf.EndOffset)
}

func initAndGetWatermark(c *kafka.Consumer, topic string) (int64, int64) {
	c.Assign([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: 0,
		Offset:    kafka.OffsetBeginning,
	}})

	msg, err := c.ReadMessage(5 * time.Second)
	if err != nil {
		panic(fmt.Sprintf("ReadMessage failed: %v", err))
	}
	fmt.Printf("第一条消息 offset: %d\n", msg.TopicPartition.Offset)

	low, high, err := c.GetWatermarkOffsets(topic, 0)
	if err != nil {
		panic(fmt.Sprintf("GetWatermarkOffsets failed: %v", err))
	}
	fmt.Printf("earliest: %d, latest: %d\n", low, high)

	return int64(low), int64(high)
}

func validateOffsetRange(conf *config.KafkaConfig, low, high int64) bool {
	if conf.StartOffset < low || conf.StartOffset > high {
		fmt.Printf("startOffset %d 不在合法范围 [%d, %d] 内，退出\n", conf.StartOffset, low, high)
		return false
	}
	if conf.EndOffset < low || conf.EndOffset > high {
		fmt.Printf("endOffset %d 不在合法范围 [%d, %d] 内，退出\n", conf.EndOffset, low, high)
		return false
	}
	if conf.StartOffset > conf.EndOffset {
		fmt.Printf("startOffset %d > endOffset %d，退出\n", conf.StartOffset, conf.EndOffset)
		return false
	}
	return true
}

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