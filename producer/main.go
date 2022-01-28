package main

import (
	"github.com/EgorBessonov/KafkaTestProj/internal/config"
	"github.com/EgorBessonov/KafkaTestProj/internal/producer"
	"github.com/caarlos0/env"
	"log"
)

const messageCount = 2000

func main() {
	cfg := config.Config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatal("can't parse configs")
	}
	prod, err := producer.NewProducer(&cfg)
	if err != nil {
		log.Fatalf("error while creation producer  - %e", err)
	}
	defer func() {
		if err = prod.Conn.Close(); err != nil {
			log.Fatalf("error while closing producer connection - %e", err)
		}
	}()
	log.Println("producer successfully created")
	err = prod.WriteMessages(messageCount)
	if err != nil {
		log.Fatalf("error while sending messages - %e", err)
	}
	log.Println("successfully send messages")
}
