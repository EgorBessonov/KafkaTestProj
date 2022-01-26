package producer

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
	kafkaDeadLine = 10
)

type Producer struct {
	Connection *kafka.Conn
}

func NewProducer(cfg config.Config) *Producer {
	conn, err := kafka.DialLeader(context.Background(), "tcp", fmt.Sprintf("%s:%s", cfg.KafkaHost, cfg.KafkaPort), cfg.KafkaTopic, 0)
	if err != nil {
		log.Fatalf("producer: connection failed - %e", err)
		return nil
	}
	err = conn.SetWriteDeadline(time.Now().Add(time.Second * kafkaDeadLine))
	if err != nil {
		log.Fatalf("producer: error while setting deadline - %e", err)
	}
	log.Println("producer: successfully created")
	return &Producer{Connection: conn}
}

func (p *Producer) PublishMessage(message *model.KafkaMessage) error {
	msg, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("producer: publishing failed - %e", err)
	}
	_, err = p.Connection.WriteMessages(kafka.Message{Value: []byte(msg)})
	return nil
}
