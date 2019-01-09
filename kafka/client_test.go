package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
)

func Test_kafkaConfigVersion(t *testing.T) {
	c := Config{}

	cfg, err := c.newKafkaConfig()
	if err != nil {
		t.Fatalf("Config should be valid: %s", err)
	}

	if cfg.Version != sarama.V1_0_0_0 {
		t.Errorf("Default version should be v1; got %s", cfg.Version)
	}
}
