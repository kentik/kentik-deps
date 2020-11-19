package datasets

import (
	"fmt"
	"strings"
)

//go:generate python protocols.py protocols.csv protocols_data.go

// return the protocol number for the given protocol name, or 255 and and error
func GetProtocolNumberFromName(name string) (uint8, error) {

	name = strings.ToLower(name)
	name = strings.Replace(name, " ", "", -1)
	name = strings.Replace(name, "_", "", -1)
	name = strings.Replace(name, "-", "", -1)
	name = strings.Replace(name, ".", "", -1)
	name = strings.Replace(name, "/", "", -1)

	num, ok := protocolNameToNumber[name]
	if !ok {
		return 255, fmt.Errorf("Unknown protocol name '%s'", name)
	}
	return num, nil
}
