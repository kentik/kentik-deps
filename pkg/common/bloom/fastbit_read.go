package bloom

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
)

const addrLen = 17

// ReadDataAddr reads a fastbit column data file into memory,
// as written by fastbit.WriteDataAddr. The binary format is
// [17]byte, where [0] is the inet family (4 or 6) and the
// rest is the v4 or v6 address. If v4, then [5:17] will be 0.
func ReadDataAddr(filename string) ([][addrLen]byte, error) {
	file, err := openFile(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return readDataAddrStream(file)
}

// Our machines all run on amd64, and so our fastbit C code that
// writes to disk elsewhere is little endian. Of course, this
// doesn't actually matter here because we are reading the data
// byte by byte.
var fastbitEndianness = binary.LittleEndian

func readDataAddrStream(stream io.Reader) ([][addrLen]byte, error) {
	bs, err := ioutil.ReadAll(stream)
	if err != nil {
		return nil, err
	}
	if len(bs)%addrLen != 0 {
		return nil, fmt.Errorf("unexpected file length (not divisible by %d)", addrLen)
	}

	addrs := make([][addrLen]byte, len(bs)/addrLen)
	err = binary.Read(bytes.NewReader(bs), fastbitEndianness, &addrs)
	if err != nil {
		return nil, err
	}
	return addrs, nil
}
