package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL string
	RedisURL    string
}

func Load() Config {
	err := godotenv.Load()

	if err != nil {
		log.Println("Warning: No .env file found (this is fine if running in production)")
	}

	return Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		RedisURL:    os.Getenv("REDIS_URL"),
	}
}
