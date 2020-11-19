package kt

import "strings"

// remove nul bytes
// not necessary after old data ages out
// Nov. 15 2019 https://github.com/kentik/streaming/pull/467
func AlertKeyFixupForGeoNulBytes(key string) string {
	return strings.Trim(key, "\x00")
}
