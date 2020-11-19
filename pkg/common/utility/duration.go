package utility

import (
	"math/rand"
	"time"
)

// RandDurationMilliseconds return a time Duration between the input milliseconds ranges
func RandDurationMilliseconds(from int, to int) time.Duration {
	if to < from {
		to = from
	}
	milliseconds := from + int(rand.Float32()*float32((to-from)))
	return time.Duration(time.Duration(milliseconds) * time.Millisecond)
}

// RandDurationSeconds return a time Duration between the input seconds ranges
func RandDurationSeconds(from int, to int) time.Duration {
	if to < from {
		to = from
	}
	milliseconds := from*1000 + int(rand.Float32()*float32((to-from)*1000))
	return time.Duration(time.Duration(milliseconds) * time.Millisecond)
}

// RandDurationMinutes returns a time Duration between the input minute ranges
func RandDurationMinutes(from int, to int) time.Duration {
	return RandDurationSeconds(from*60, to*60)
}

// RandDurationHours returns a time Duration between the input hour ranges
func RandDurationHours(from int, to int) time.Duration {
	return RandDurationMinutes(from*60, to*60)
}
