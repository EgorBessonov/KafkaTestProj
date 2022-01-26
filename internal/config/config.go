package config

type Config struct {
	PostgresdbURL string `env:"POSTGRESDB_URL"`
	KafkaPort     string `env:"KAFKAPORT"`
	KafkaHost     string `env:"KAFKAHOST"`
	KafkaTopic    string `env:"KAFKATOPIC"`
	KafkaGroupID  string `env:"KafkaGID"`
}
