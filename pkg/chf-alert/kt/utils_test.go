package kt

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleLineElide(t *testing.T) {
	assert.Equal(t, "hello", SingleLineElide("hello", -1))
	assert.Equal(t, "hello", SingleLineElide("hello\r\n", -1))

	assert.Equal(t, "hello", SingleLineElide("hello", 100))
	assert.Equal(t, "hello", SingleLineElide("hello", 5))
	assert.Equal(t, "h...", SingleLineElide("hello", 4))
	assert.Equal(t, "...", SingleLineElide("hello", 3))
	assert.Equal(t, "hello", SingleLineElide("hello", 2))

	assert.Equal(t, "hello", SingleLineElide("\r\n\r\nhello\r\n\n", 5))
	assert.Equal(t, "h...", SingleLineElide("\r\n\r\nhello", 4))
}

func TestJSONNumberInt(t *testing.T) {
	tcs := []struct {
		json          string
		expectedOK    bool
		expectedValue int
	}{
		{`99`, true, 99},
		{`99.5`, true, 99},
		{`99.999`, true, 99},
		{`99.0`, true, 99},
		{`20.0`, true, 20},
		{`0.001`, true, 0},
		{`"99"`, false, 0}, // Please no.
	}

	for _, tc := range tcs {
		var i JSONNumberInt
		err := json.Unmarshal([]byte(tc.json), &i)
		if tc.expectedOK {
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedValue, int(i))
		} else {
			assert.Error(t, err)
		}
	}
}

func TestJSONStringOrNumberString(t *testing.T) {
	tcs := []struct {
		json          string
		expectedOK    bool
		expectedValue string
	}{
		{`"foobar"`, true, "foobar"},
		{`"99"`, true, "99"},
		{`99`, true, "99"},
		{`99.999`, true, "99.999"},
		{`99.00`, true, "99.00"},
		{`0.001`, true, "0.001"},
		{`asdf`, false, ""}, // Invalid JSON.
	}

	for _, tc := range tcs {
		var s JSONStringOrNumberString
		err := json.Unmarshal([]byte(tc.json), &s)
		if tc.expectedOK {
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedValue, string(s))
		} else {
			assert.Error(t, err)
		}
	}
}
