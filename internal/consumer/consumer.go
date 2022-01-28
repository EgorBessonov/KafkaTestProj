package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/EgorBessonov/KafkaTestProj/internal/config"
	"github.com/EgorBessonov/KafkaTestProj/internal/model"
	"github.com/jackc/pgx/v4"
	"github.com/segmentio/kafka-go"
)

const (
	minBytes = 10e3
	maxBytes = 10e6
)

type Consumer struct {
	Conn *kafka.Conn
}

func NewConsumer(cfg *config.Config) (*Consumer, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", fmt.Sprintf("%s:%s", cfg.KafkaHost, cfg.KafkaPort), cfg.KafkaTopic, 0)

	if err != nil {
		return nil, fmt.Errorf("consumer: can't create new instance - %e", err)
	}
	return &Consumer{Conn: conn}, nil
}

func (consumer Consumer) Read(pBatch *pgx.Batch) error {
	batch := consumer.Conn.ReadBatch(minBytes, maxBytes)
	batchArr := make([]byte, 10e3)
	for {
		n, err := batch.Read(batchArr)
		if err != nil {
			break
		}
		message := model.KafkaMessage{}
		messageString := string(batchArr[:n])
		err = json.Unmarshal([]byte(messageString), &message)
		if err != nil {
			return fmt.Errorf("consumer: can't parse message")
		}
		pBatch.Queue("insert into kafka(message, messagetime) values($1, $2)", message.Message, message.Time)
	}

	if err := batch.Close(); err != nil {
		return fmt.Errorf("consumer: error while closing batch - %e", err)
	}
	return nil
}
