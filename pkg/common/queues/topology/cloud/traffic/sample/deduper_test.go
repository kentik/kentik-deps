package sample

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeduper(t *testing.T) {
	sut := NewDeduper()
	assert.False(t, sut.LikelySeenMessage(1234, 1111111, time.Now()))
	assert.True(t, sut.LikelySeenMessage(1234, 1111111, time.Now()))
	assert.False(t, sut.LikelySeenMessage(1235, 1111111, time.Now()))

	assert.False(t, sut.LikelySeenMessage(5555, 3333333, time.Now().Add(-100*time.Minute)))
	assert.True(t, sut.LikelySeenMessage(5555, 3333333, time.Now().Add(-99*time.Minute)))
	assert.True(t, sut.LikelySeenMessage(5555, 3333333, time.Now().Add(-98*time.Minute)))
	assert.True(t, sut.LikelySeenMessage(5555, 3333333, time.Now().Add(-97*time.Minute)))
	assert.False(t, sut.LikelySeenMessage(5555, 3333333, time.Now().Add(-94*time.Minute)))

	ninetyThreeMinutesAgo := time.Now().Add(-93 * time.Minute)
	assert.True(t, sut.LikelySeenMessage(5555, 3333333, ninetyThreeMinutesAgo))
	assert.True(t, sut.LikelySeenMessage(5555, 3333333, time.Now().Add(-94*time.Minute)))
	assert.Equal(t, ninetyThreeMinutesAgo.Unix(), sut.seenMessages[SeenKey{CompanyID: 5555, RandInt: 3333333}])

	assert.Equal(t, 2, sut.Cleanup())
	assert.False(t, sut.LikelySeenMessage(5555, 3333333, time.Now().Add(-93*time.Minute)))
}
