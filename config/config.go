package config

import (
    "fmt"
    "os"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "gopkg.in/yaml.v3"
)

var (
    C         *kafka.Consumer
    KafkaConf *KafkaConfig
)

type KafkaConfig struct {
    KafkaConfig struct {
        BootstrapServers []string `yaml:"bootstrap.servers"`
        GroupID          string   `yaml:"group.id"`
        AutoOffsetReset  string   `yaml:"auto.offset.reset"`
        SecurityProtocol string   `yaml:"security.protocol"`
        SaslMechanisms   string   `yaml:"sasl.mechanisms"`
        SaslUsername     string   `yaml:"sasl.username"`
        SaslPassword     string   `yaml:"sasl.password"`
    } `yaml:"kafkaConfig"`

    Topic       string `yaml:"topic"`
    StartOffset int64  `yaml:"startOffset"`
    EndOffset   int64  `yaml:"endOffset"`
}

func init() {
    cfgPath := "/app/config.yml"

    data, err := os.ReadFile(cfgPath)
    if err != nil {
        panic(fmt.Sprintf("read config file failed: %v", err))
    }

    KafkaConf = &KafkaConfig{}
    err = yaml.Unmarshal(data, KafkaConf)
    if err != nil {
        panic(fmt.Sprintf("parse config file failed: %v", err))
    }

    if len(KafkaConf.KafkaConfig.BootstrapServers) == 0 {
        panic("bootstrap.servers is required")
    }
    if KafkaConf.Topic == "" {
        panic("topic is required")
    }

    servers := ""
    for i, s := range KafkaConf.KafkaConfig.BootstrapServers {
        if i > 0 {
            servers += ","
        }
        servers += s
    }

    C, err = kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers":  servers,
        "group.id":           KafkaConf.KafkaConfig.GroupID,
        "auto.offset.reset":  KafkaConf.KafkaConfig.AutoOffsetReset,
        "security.protocol":  KafkaConf.KafkaConfig.SecurityProtocol,
        "sasl.mechanisms":    KafkaConf.KafkaConfig.SaslMechanisms,
        "sasl.username":      KafkaConf.KafkaConfig.SaslUsername,
        "sasl.password":      KafkaConf.KafkaConfig.SaslPassword,
    })
    if err != nil {
        panic(fmt.Sprintf("create kafka consumer failed: %v", err))
    }
}

func Close() {
    if C != nil {
        C.Close()
    }
}