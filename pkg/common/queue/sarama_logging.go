package queue

import (
	"github.com/Shopify/sarama"
)

// Logger represents an interface around https://github.com/kentik/golog
type Logger interface {
	Debugf(prefix, format string, v ...interface{})
	Infof(prefix, format string, v ...interface{})
	Errorf(prefix, format string, v ...interface{})
	Warnf(prefix, format string, v ...interface{})
}

// SaramaLogger receives log statements from the sarama library
type SaramaLogger struct {
	log       Logger
	logPrefix string
}

// Print prints a log statement
func (s *SaramaLogger) Print(v ...interface{}) {
	s.log.Infof("(sarama) ", "%s", v)
}

// Printf prints a log statement
func (s *SaramaLogger) Printf(format string, v ...interface{}) {
	s.log.Infof("(sarama) ", format, v...)
}

// Println prints a log statement
func (s *SaramaLogger) Println(v ...interface{}) {
	s.log.Infof("(sarama) ", "%v", v)
}

// RegisterSaramaLogger registers our logger to receive log statements from sarama
func RegisterSaramaLogger(log Logger, logPrefix string) {
	sarama.Logger = &SaramaLogger{
		log:       log,
		logPrefix: logPrefix,
	}
}
