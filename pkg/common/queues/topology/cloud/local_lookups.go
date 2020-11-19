package cloud

import (
	"strings"
)

// LocalLookups hold uint32->string lookups that exist only within the scope
// of this running application, and are never persisted.
type LocalLookups struct {
	fromInt      map[uint32]string
	fromStrLower map[string]uint32
	maxVal       uint32
}

// NewLocalLookups returns a new LocalLookups
func NewLocalLookups() *LocalLookups {
	return &LocalLookups{
		fromInt:      make(map[uint32]string),
		fromStrLower: make(map[string]uint32),
	}
}

// GetOrRegisterLookup returns a uint32 lookup for the input string
// return 0 if empty string
func (l *LocalLookups) GetOrRegisterLookup(str string) uint32 {
	if str == "" {
		return 0
	}
	toLower := strings.ToLower(str)
	if val, ok := l.fromStrLower[toLower]; ok {
		if l.fromInt[val] != str {
			// update the casing
			l.fromInt[val] = str
		}
		return val
	}

	l.maxVal++
	l.fromStrLower[toLower] = l.maxVal
	l.fromInt[l.maxVal] = str

	return l.maxVal
}

// GetStrFromLookup returns the string from the input lookup
func (l *LocalLookups) GetStrFromLookup(lookup uint32) string {
	return l.fromInt[lookup]
}

// AllLookups returns a map of all uint32->string
func (l *LocalLookups) AllLookups() map[uint32]string {
	return l.fromInt
}
