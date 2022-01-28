package main

import (
	"context"
	"fmt"
	"github.com/EgorBessonov/KafkaTestProj/internal/config"
	"github.com/EgorBessonov/KafkaTestProj/internal/consumer"
	"github.com/EgorBessonov/KafkaTestProj/internal/repository"
	"github.com/caarlos0/env"
	"log"
	"time"
)

const (
	consumerDeadLine = 10
)

func main() {
	cfg := config.Config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatal("can't parse configs")
	}
	cons, err := consumer.NewConsumer(&cfg)
	if err != nil {
		log.Fatalf("error while creation consumer  - %e", err)
	}
	repos := repository.NewPostgresRepository(&cfg)
	defer func() {
		if err = cons.Conn.Close(); err != nil {
			log.Fatalf("error while closing consumer connection - %e", err)
		}
	}()
	log.Println("consumer successfully created")
	readProcess := make(chan bool)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*consumerDeadLine)
	defer cancel()
	go func() {
		for {
			select {
			case <-ctx.Done():
				readProcess <- false
				return
			default:
				err := cons.Read(repos)
				if err != nil {
					readProcess <- false
					log.Fatalf("can't read messages - %e", err)
					return
				}
			}
		}
	}()
	fmt.Println(<-readProcess)
}
