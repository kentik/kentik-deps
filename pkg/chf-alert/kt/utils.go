package kt

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func SingleLineElide(s string, maxLen int) string {
	s = strings.Replace(s, "\n", "", -1)
	s = strings.Replace(s, "\r", "", -1)
	if len(s) > maxLen && maxLen >= 3 {
		s = s[:maxLen-3] + "..."
	}
	return s
}

func ParsePortFromListenAddressOrPanic(listenAddress string) uint16 {
	listenAddrSplit := strings.Split(listenAddress, ":")
	var port int
	if len(listenAddrSplit) == 2 { // Expecting a string like "hostname:1234"
		port, _ = strconv.Atoi(listenAddrSplit[1])
	}
	if port == 0 {
		panic(fmt.Sprintf("couldn't parse port from listen address: %s", listenAddress))
	}
	return uint16(port)
}

type JSONNumberInt int

func (i *JSONNumberInt) UnmarshalJSON(b []byte) error {
	// First try unmarshalling as an int.
	err := json.Unmarshal(b, (*int)(i))
	if err == nil {
		return nil // Success. Normal, desired case.
	}
	// If that doesn't work, try interpreting as a float and truncate it.
	var v float64
	err = json.Unmarshal(b, &v)
	if err == nil {
		*i = JSONNumberInt(v)
	}
	return err
}

type JSONStringOrNumberString string

func (s *JSONStringOrNumberString) UnmarshalJSON(b []byte) error {
	// First try unmarshalling as a string.
	err := json.Unmarshal(b, (*string)(s))
	if err == nil {
		return nil // Success. Normal, desired case.
	}

	// If that doesn't work, try interpreting as an int.
	var vint int
	err = json.Unmarshal(b, &vint)
	if err == nil {
		*s = JSONStringOrNumberString(b) // Exactly as it was in the JSON representation
		return nil
	}

	// If that doesn't work, try interpreting as a float.
	var vfloat float64
	err = json.Unmarshal(b, &vfloat)
	if err == nil {
		*s = JSONStringOrNumberString(b) // Exactly as it was in the JSON representation
		return nil
	}
	return err
}

// A FlexInt is an int that can be unmarshalled from a JSON field
// that has either a number or a string value.
// E.g. if the json field contains an string "42", the
// FlexInt value will be "42".
type FlexInt int

// UnmarshalJSON implements the json.Unmarshaler interface, which
// allows us to ingest values of any json type as an int and run our custom conversion

func (fi *FlexInt) UnmarshalJSON(b []byte) error {
	if b[0] != '"' {
		return json.Unmarshal(b, (*int)(fi))
	}
	if len(b) <= 2 { // empty string
		*fi = 0
		return nil
	}

	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*fi = FlexInt(i)
	return nil
}

// FlexBool doesn't fail deserialization even if somebody stored empty string in this field
type FlexBool bool

// UnmarshalJSON does what UnmarshalJSON does the best
func (fb *FlexBool) UnmarshalJSON(b []byte) error {
	if b[0] != '"' {
		return json.Unmarshal(b, (*bool)(fb))
	}

	if len(b) <= 2 {
		return nil
	}

	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	if strings.ToLower(s) == "true" {
		*fb = true
		return nil
	}

	return nil
}

// FlexUint64 deserializes from both integer representation and string
// empty string is treated as 0
type FlexUint64 uint64

// UnmarshalJSON does what UnmarshalJSON does the best
func (fu *FlexUint64) UnmarshalJSON(b []byte) error {
	if b[0] != '"' {
		return json.Unmarshal(b, (*uint64)(fu))
	}

	if len(b) <= 2 {
		return nil
	}

	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return err
	}

	*fu = FlexUint64(val)
	return nil
}

func GetenvFallback(key, fallbackKey string) string {
	if v, ok := os.LookupEnv(key); !ok {
		return os.Getenv(fallbackKey)
	} else {
		return v
	}
}

func StrOrDef(s, def string) string { return IfElseStr(s != "", s, def) }
func IfElseStr(pred bool, a, b string) string {
	if pred {
		return a
	}
	return b
}

func IntOrDef(v, def int) int { return IfElseInt(v != 0, v, def) }
func IfElseInt(pred bool, a, b int) int {
	if pred {
		return a
	}
	return b
}

func DurOrDef(v, def time.Duration) time.Duration { return IfElseDur(v != 0, v, def) }
func IfElseDur(pred bool, a, b time.Duration) time.Duration {
	if pred {
		return a
	}
	return b
}

func AtoiDefaultOrPanic(s string, def int) int {
	if s == "" {
		return def
	}
	return AtoiOrPanic(s)
}

func AtoiOrPanic(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return i
}

func ParseDurationDefaultOrPanic(s string, def time.Duration) time.Duration {
	d, err := ParseDurationDefault(s, def)
	if err != nil {
		panic(err)
	}
	return d
}

func ParseBoolDefaultOrPanic(s string, def bool) bool {
	d, err := ParseBoolDefault(s, def)
	if err != nil {
		panic(err)
	}
	return d
}

func ParseDurationDefault(s string, def time.Duration) (time.Duration, error) {
	if s == "" {
		return def, nil
	}
	return time.ParseDuration(s)
}

func ParseBoolDefault(s string, def bool) (bool, error) {
	if s == "" {
		return def, nil
	}
	return strconv.ParseBool(s)
}

func EscapeColonInString(input string) string {
	return strings.Replace(input, ":", "_", -1)
}
