package config

type Config struct {
	PostgresdbURL string `env:"POSTGRESDB_URL" envDefault:"postgresql://postgres:passwd@localhost:5614/crudserverdb"`
	KafkaPort     string `env:"KAFKAPORT" envDefault:"9092"`
	KafkaHost     string `env:"KAFKAHOST" envDefault:"localhost"`
	KafkaTopic    string `env:"KAFKATOPIC" envDefault:"test"`
	KafkaGroupID  string `env:"KafkaGID" envDefault:"KafkaTestGroup"`
}
