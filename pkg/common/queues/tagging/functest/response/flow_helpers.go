package response

import (
	"bytes"
	"fmt"
	"io/ioutil"

	capn "github.com/glycerine/go-capnproto"
	chf "github.com/kentik/proto/kflow"
)

func deserializeFlow(msg []byte) (chf.CHF, error) {
	dcompOut := bytes.NewBuffer(msg)
	dser, err := ioutil.ReadAll(dcompOut)

	nnd := bytes.NewBuffer(dser)
	src, err := capn.ReadFromStream(nnd, nil)
	if err != nil {
		return chf.CHF{}, fmt.Errorf("Error deserializing flow record: error reading from stream: %v", err)
	}

	// unpack the flow
	flows := chf.ReadRootPackedCHF(src).Msgs().ToArray()
	if len(flows) != 1 {
		return chf.CHF{}, fmt.Errorf("Expected 1 flow record in response message, found %d", len(flows))
	}

	return flows[0], nil
}

// serialize the flow into a list of one
func serializeFlow(flow chf.CHF) ([]byte, error) {
	segT := capn.NewBuffer(nil)
	listWrap := chf.NewRootPackedCHF(segT)
	chfList := chf.NewCHFList(segT, 1)
	chfList.Set(0, flow)
	listWrap.SetMsgs(chfList)

	buf := bytes.Buffer{}
	if _, err := segT.WriteTo(&buf); err != nil {
		return nil, fmt.Errorf("Error writing flows to buffer: %s", err)
	}

	return buf.Bytes(), nil
}
