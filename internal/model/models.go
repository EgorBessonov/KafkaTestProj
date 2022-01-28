package model

import "time"

type KafkaMessage struct {
	Message string
	Time    time.Time
}

type KafkaMessages struct {
	messages []*KafkaMessage
}
