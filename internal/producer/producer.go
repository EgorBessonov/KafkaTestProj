package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/EgorBessonov/KafkaTestProj/internal/config"
	"github.com/EgorBessonov/KafkaTestProj/internal/model"
	"github.com/segmentio/kafka-go"
	"strconv"
	"time"
)

const (
	writeDeadLine = 2
)

type Producer struct {
	Conn *kafka.Conn
}

func NewProducer(cfg *config.Config) (*Producer, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", fmt.Sprintf("%s:%s", cfg.KafkaHost, cfg.KafkaPort), cfg.KafkaTopic, 0)
	if err != nil {
		return nil, fmt.Errorf("producer: can't create new instance - %e", err)
	}
	return &Producer{Conn: conn}, nil
}

func (producer Producer) WriteMessages(messagesCount int) error {
	var messages []kafka.Message
	for i := 1; i <= messagesCount; i++ {
		message := model.KafkaMessage{
			Message: "message #" + strconv.Itoa(i),
			Time:    time.Now(),
		}
		msg, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("producer: can't parse message - %e", err)
		}
		messages = append(messages, kafka.Message{Value: msg})
	}
	err := producer.Conn.SetWriteDeadline(time.Now().Add(time.Second * writeDeadLine))
	if err != nil {
		return fmt.Errorf("producer: deadline error - %e", err)
	}
	_, err = producer.Conn.WriteMessages(messages...)
	if err != nil {
		return fmt.Errorf("producer: can't send messages -%e", err)
	}
	return nil
}
