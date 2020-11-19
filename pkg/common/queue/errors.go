package queue

import (
	"errors"
	"fmt"
)

// ErrConsumerStopped indicates a consumer has been stopped
var ErrConsumerStopped = errors.New("Consumer has been stopped - no more messages")

// ErrNoMoreMessages represents the case where we try to read from the topic, but we've reached the end
var ErrNoMoreMessages = fmt.Errorf("No more messages to read")
