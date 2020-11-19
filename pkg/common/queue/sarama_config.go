package queue

import (
	"github.com/Shopify/sarama"
)

// SaramaConfigModFunc allows a caller to finely tune the sarama client config
type SaramaConfigModFunc func(*sarama.Config)
