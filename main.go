package main

import (
	"fmt"
	"github.com/EgorBessonov/KafkaTestProj/internal/broker/consumer"
	"github.com/EgorBessonov/KafkaTestProj/internal/broker/producer"
	"github.com/EgorBessonov/KafkaTestProj/internal/config"
	"github.com/EgorBessonov/KafkaTestProj/internal/repository"
	"github.com/EgorBessonov/KafkaTestProj/internal/service"
	"github.com/caarlos0/env"
	"log"
)

const (
	messagesCount = 2000
)

func main() {
	cfg := config.Config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatal("config parsing failed")
	}
	cons := consumer.NewConsumer(cfg)
	prod := producer.NewProducer(cfg)
	rps := repository.NewPostgresRepository(cfg)
	defer func() {
		if err := cons.Reader.Close(); err != nil {
			log.Fatalf("consumer: error while closing reader - %e", err)
		}
		if err := prod.Connection.Close(); err != nil {
			log.Fatalf("producer: error while closing connection - %e", err)
		}
		rps.DBconn.Close()
	}()
	s := service.NewService(rps, cons, prod)
	send := make(chan bool)
	read := make(chan bool)
	go func() {
		s.SendMessages(messagesCount)
		send <- true
	}()
	go func() {
		s.ReadMessages(messagesCount)
		read <- true
	}()
	fmt.Println(<-send)
	fmt.Println(<-read)
}
