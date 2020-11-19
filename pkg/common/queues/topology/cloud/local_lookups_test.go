package cloud

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLocalLookups(t *testing.T) {
	assert := assert.New(t)
	sut := NewLocalLookups()
	assert.Equal(uint32(1), sut.GetOrRegisterLookup("hi"))
	assert.Equal("hi", sut.GetStrFromLookup(1))
	assert.Equal(uint32(1), sut.GetOrRegisterLookup("Hi"))
	assert.Equal("Hi", sut.GetStrFromLookup(1))

	assert.Equal("", sut.GetStrFromLookup(2))
	assert.Equal(uint32(2), sut.GetOrRegisterLookup("Hello"))
	assert.Equal("Hello", sut.GetStrFromLookup(2))

	assert.Equal(uint32(0), sut.GetOrRegisterLookup(""))
}
