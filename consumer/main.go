package main

import (
	"context"
	"github.com/EgorBessonov/KafkaTestProj/internal/config"
	"github.com/EgorBessonov/KafkaTestProj/internal/consumer"
	"github.com/EgorBessonov/KafkaTestProj/internal/repository"
	"github.com/caarlos0/env"
	"github.com/jackc/pgx/v4"
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
	batchChnl := make(chan *pgx.Batch)
	batchExecEnd := make(chan bool)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*consumerDeadLine)
	defer cancel()
	go func() {
		for {
			batch := pgx.Batch{}
			select {
			case <-ctx.Done():
				return
			default:
				err := cons.Read(&batch)
				if err != nil {
					readProcess <- false
					log.Fatalf("can't read messages - %e", err)
					return
				}
				batchChnl <- &batch
				readProcess <- true

			}
		}
	}()
	go func() {
		var ok bool
		for {
			ok = <-readProcess
			switch ok {
			case ok:
				batch := <-batchChnl
				err := repos.SendBatch(context.Background(), batch)
				if err != nil {
					log.Fatalf("error while executing batch opeations - %e", err)
					return
				}
			case !ok:
				batchExecEnd <- true
				return
			}
		}
	}()
	if <-batchExecEnd {
		log.Println("successfully read and insert all messages")
	}
}
