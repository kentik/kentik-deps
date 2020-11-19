package bloom

import "os"

// SecondaryIndexesEnabled checks env vars to see if secondary
// indexes should be used. True by default.
func SecondaryIndexesEnabled() bool {
	// Secondary indexes are enabled by default.
	return !(os.Getenv(DisableSecondaryIndexesKey) == DisableSecondaryIndexesValue)
}

// Env vars and values expected for disabling secondary indexes.
const (
	DisableSecondaryIndexesKey   = "KENTIK_DISABLE_SECONDARY_INDEXES"
	DisableSecondaryIndexesValue = "true"
)
