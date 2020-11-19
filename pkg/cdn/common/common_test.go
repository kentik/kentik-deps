package common

import (
	"testing"

	"github.com/kentik/golog/logger"
	"github.com/stretchr/testify/assert"
)

var (
	l = logger.New(logger.Levels.Debug)
)

func init() {
	logger.SetStdOut()
}

func TestReadREs(t *testing.T) {
	res, err := ReadREs(l, "", "../../cdns.csv")

	assert.NoError(t, err)
	assert.Equal(t, 52, len(res))
}
