package kt

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultIPAuthorizer(t *testing.T) {
	for _, tc := range []struct {
		ip       string
		expected bool
	}{
		{"192.168.0.1", false},
		{"127.0.0.1", false},
		{"10.1.1.25", false},
		{"8.8.8.8", true},
		{"1.1.1.1", false},
	} {
		assert.Equal(
			t,
			tc.expected,
			DefaultIPAuthorizer.IsIPAuthorized(mustParseIP(tc.ip)),
			fmt.Sprintf("ip=%s expected=%t", tc.ip, tc.expected))
	}
}

func mustParseIP(s string) net.IP {
	ip := net.ParseIP(s)
	if ip == nil {
		panic(fmt.Sprintf("ParseIP(%s) is nil", s))
	}
	return ip
}
