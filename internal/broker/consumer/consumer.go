package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/EgorBessonov/KafkaTestProj/internal/config"
	"github.com/EgorBessonov/KafkaTestProj/internal/model"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

const (
	kafkaMinBytes = 10e3
	kafkaMaxBytes = 10e6
)

type Consumer struct {
	Reader *kafka.Reader
}

func NewConsumer(cfg config.Config) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{fmt.Sprintf("%s:%s", cfg.KafkaHost, cfg.KafkaPort)},
		GroupID:        cfg.KafkaGroupID,
		Topic:          cfg.KafkaTopic,
		MinBytes:       kafkaMinBytes,
		MaxBytes:       kafkaMaxBytes,
		CommitInterval: time.Second,
	})
	log.Println("consumer: successfully created")
	return &Consumer{Reader: reader}
}

func (c *Consumer) Read(ctx context.Context) (*model.KafkaMessage, error) {
	m, err := c.Reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("consumer: can't read message - %e", err)
	}
	message := model.KafkaMessage{}
	err = json.Unmarshal(m.Value, &message)
	if err != nil {
		return nil, fmt.Errorf("consumer: error while parsing message - %e", err)
	}
	return &message, nil
}
