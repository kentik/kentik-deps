package sample

import (
	"time"
)

// Deduper figures out if we've already seen a message, based on the RandInt set by the producer
type Deduper struct {
	// map of CompanyID/RandInt->message date in unix seconds
	seenMessages map[SeenKey]int64
}

// NewDeduper returns a new Deduper
func NewDeduper() *Deduper {
	return &Deduper{
		seenMessages: make(map[SeenKey]int64),
	}
}

// LikelySeenMessage returns whether we've likely seen this message before
func (d *Deduper) LikelySeenMessage(companyID uint32, randInt uint64, messageTime time.Time) bool {
	if randInt == 0 {
		// likely an older message with no randInt set - do nothing
		return false
	}

	messageUnix := messageTime.Unix()

	// we'll consider a message seen if we've seen the same random int for this companyID
	// within the last 5 minutes
	key := SeenKey{CompanyID: companyID, RandInt: randInt}
	lastSeenUnix := d.seenMessages[key]

	// remember that we've seen this random int at this timestamp
	if messageUnix > lastSeenUnix {
		d.seenMessages[key] = messageUnix
	}

	// return whether we've seen this random key for this company within the last 120 seconds
	if messageUnix > lastSeenUnix {
		return messageUnix-lastSeenUnix <= 120
	}
	return lastSeenUnix-messageUnix <= 120
}

// Cleanup cleans up what cache it can - this should be called every once in a while (order of minutes)
// to prevent too many entries in the cache.
// - returns how many entries in cache
func (d *Deduper) Cleanup() int {
	now := time.Now().Unix()

	for k, v := range d.seenMessages {
		// delete any cache older than 10 minutes (600 seconds)
		if now-v > 600 {
			delete(d.seenMessages, k)
		}
	}

	return len(d.seenMessages)
}

// SeenKey holds a companyID and random int we've seen for a company
type SeenKey struct {
	CompanyID uint32
	RandInt   uint64
}
