package kt

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeviceIDSet_UnmarshalJSON(t *testing.T) {
	type container struct {
		Devices *DeviceIDSet `json:"devices"`
	}

	j := `{ "devices":  [1, 2, 3, 4, 1, 2, 3, 4] }`
	c := container{}
	err := json.Unmarshal([]byte(j), &c)

	assert.NoError(t, err)
	assert.Len(t, c.Devices.Items(), 4)
	assert.Equal(t, []DeviceID{1, 2, 3, 4}, c.Devices.Items())
}
