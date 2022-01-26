package service

import (
	"context"
	"github.com/EgorBessonov/KafkaTestProj/internal/broker/consumer"
	"github.com/EgorBessonov/KafkaTestProj/internal/broker/producer"
	"github.com/EgorBessonov/KafkaTestProj/internal/model"
	"github.com/EgorBessonov/KafkaTestProj/internal/repository"
	"github.com/jackc/pgx/v4"
	"log"
	"time"
)

const (
	batchDuration = 60
)

type Service struct {
	Repository *repository.PostgresRepository
	Consumer   *consumer.Consumer
	Producer   *producer.Producer
}

func NewService(rps *repository.PostgresRepository, cons *consumer.Consumer, prod *producer.Producer) *Service {
	return &Service{Repository: rps, Consumer: cons, Producer: prod}
}

func (s *Service) ReadAndBatch(batch pgx.Batch, messagesCount int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(3*messagesCount))
	defer cancel()
	for i := 0; i < messagesCount; i++ {
		message, err := s.Consumer.Read(ctx)
		if err != nil {
			log.Fatal("service: error while reading - %e", err)
			return err
		}
		batch.Queue(`insert into kafka (message, messageTime)
		values ($1, $2)`, message.Message, message.Time)
	}
	return nil
}

func (s *Service) SendMessages(messagesCount int) {
	for i := 0; i < messagesCount; i++ {
		message := model.KafkaMessage{
			Message: "message #" + string(i),
			Time:    time.Now(),
		}
		err := s.Producer.PublishMessage(&message)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (s *Service) ReadMessages(messagesCount int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*batchDuration)
	defer cancel()
	batch := pgx.Batch{}
	err := s.ReadAndBatch(batch, messagesCount)
	if err != nil {
		log.Fatalf("service: error while reading messages - %e", err)
	}
	err = s.Repository.SendBatch(ctx, &batch)
	if err != nil {
		log.Fatalf("service: error while reading messages - %e", err)
	}
}
