package repository

import (
	"context"
	"github.com/EgorBessonov/KafkaTestProj/internal/config"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
)

type PostgresRepository struct {
	DBconn *pgxpool.Pool
}

func NewPostgresRepository(cfg *config.Config) *PostgresRepository {
	conn, err := pgxpool.Connect(context.Background(), cfg.PostgresdbURL)
	if err != nil {
		log.Fatalf("repository: connection failed - %e", err)
	}
	log.Println("repository: successfully connected")
	return &PostgresRepository{DBconn: conn}
}

func (rps *PostgresRepository) SendBatch(ctx context.Context, batch *pgx.Batch) error {
	res := rps.DBconn.SendBatch(ctx, batch)
	if err := res.Close(); err != nil {
		log.Fatalf("postgres: batch error - %e", err)
		return err
	}
	return nil
}
