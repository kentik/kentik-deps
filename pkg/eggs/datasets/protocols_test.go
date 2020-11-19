package datasets

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetProtocolNumberFromName(t *testing.T) {

	val, err := GetProtocolNumberFromName("tcp")
	assert.Nil(t, err, "GetProtocolNumberFromName should know about tcp")
	assert.Equal(t, uint8(6), val, "tcp is proto number 6")

	val, err = GetProtocolNumberFromName("ipv6-icmp")
	assert.Nil(t, err, "GetProtocolNumberFromName should know about ipv6-icmp")
	assert.Equal(t, uint8(58), val, "ipv6-icmp is proto number 58")

	_, err = GetProtocolNumberFromName("teeceepee")
	assert.NotNil(t, err, "GetProtocolNumberFromName should not know about bogus protocol name teeceepee")
}
