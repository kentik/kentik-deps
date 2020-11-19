package kt

import (
	"encoding/json"
	"testing"
)

func TestMaybeasnToUint32(t *testing.T) {
	tcs := []struct {
		name       string
		input      string
		expected   uint32
		shouldFail bool
	}{
		{
			name:       "empty string fails",
			shouldFail: true,
		},
		{
			name:       "gibberish fails",
			input:      "Zqweq",
			shouldFail: true,
		},
		{
			name:     "uint32 passes",
			input:    "1234",
			expected: 1234,
		},
		{
			name:     "hex passes",
			input:    "beef",
			expected: 0xbeef,
		},
		{
			name:       "halfassed asndot fails",
			input:      "1.",
			shouldFail: true,
		},
		{
			name:     "properly formated asndot passes ",
			input:    "6.3234",
			expected: 396450,
		},
		{
			name:     "asndot with uint16.max passes",
			input:    "1.65535",
			expected: 0x1ffff,
		},
		{
			name:       "asndot with uint16.max+1 on low bytes fails",
			input:      "1.65536",
			shouldFail: true,
		},
		{
			name:       "asndot with uint16.max+1 on high bytes fails",
			input:      "65536.1",
			shouldFail: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			res, err := maybeasnToUint32(tc.input)
			if (err != nil) != tc.shouldFail {
				t.Fatalf("Unexpected error: %v", err)
			}

			if res != tc.expected {
				t.Errorf("Mismatch (expected: %x, got: %x)", tc.expected, res)
			}
		})
	}
}

func TestASNumberUnmarshal(t *testing.T) {
	tcs := []struct {
		name       string
		payload    string
		expected   ASNumber
		shouldFail bool
	}{
		{
			name:       "empty stuff fails",
			shouldFail: true,
		},
		{
			name:       "empty string value fails",
			payload:    `{"v":""}`,
			shouldFail: true,
		},
		{
			name:       "garbage string value fails",
			payload:    `{"v":"asqwe"}`,
			shouldFail: true,
		},
		{
			name:     "decimal uint value passes",
			payload:  `{"v":0}`,
			expected: 0,
		},
		{
			name:     "decimal uint.max value passes",
			payload:  `{"v":65535}`,
			expected: 0xffff,
		},
		{
			name:     "float is treated as asdot",
			payload:  `{"v":1.2}`,
			expected: 0x10002,
		},
	}

	type payload struct {
		V ASNumber `json:"v"`
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p := &payload{}
			err := json.Unmarshal([]byte(tc.payload), p)
			if (err != nil) != tc.shouldFail {
				t.Fatalf("Unexpected error: %v", err)
			}

			if p.V != tc.expected {
				t.Errorf("Mismatch (expected: %x, got: %x)", tc.expected, p.V)
			}
		})
	}
}
