package resolv

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetResolv(t *testing.T) {
	assert := assert.New(t)

	addressList := []string{"http://127.0.0.1", "http://127.0.0.2", "http://127.0.0.3"}
	target := "FOO"

	r := NewBasicResolver(strings.Join(addressList, ","), nil)
	assert.NotNil(r)

	w, err := r.Resolve(target)
	assert.NotNil(w)
	assert.Nil(err)

	updates, err := w.Next()
	assert.NotNil(updates)
	assert.Nil(err)

	assert.Equal(len(updates), len(addressList))
	for i, a := range addressList {
		assert.Equal(a, updates[i].Addr)
	}

	timeout := time.NewTimer(1 * time.Second)

	data := make(chan int, 2)

	go func() {
		updates, _ := w.Next()
		data <- len(updates)
	}()

	select {
	case res := <-data:
		assert.True(false, fmt.Sprintf("Next() returned new results -- %d", res))

	case _ = <-timeout.C:
		assert.True(true, "Next() Timeout before result returned")
	}
}
