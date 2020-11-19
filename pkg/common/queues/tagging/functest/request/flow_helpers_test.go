package request

import (
	"testing"

	capn "github.com/glycerine/go-capnproto"
	chf "github.com/kentik/proto/kflow"
	"github.com/stretchr/testify/assert"
)

func TestSerializeFlow(t *testing.T) {
	assert := assert.New(t)

	flow := chf.NewCHF(capn.NewBuffer(nil))
	flow.SetSrcAs(999)
	serializedBytes, err := serializeFlow(flow)
	assert.NoError(err)

	deserializedFlow, err := deserializeFlow(serializedBytes)
	assert.NoError(err)

	assert.Equal(uint32(999), deserializedFlow.SrcAs())
}
