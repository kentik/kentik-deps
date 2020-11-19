package client

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kentik/golog/logger"
	"github.com/kentik/tagstreaming/models/lookups"
	"github.com/stretchr/testify/assert"
)

var _lookupID1 = uint64(1000000000)
var _lookupID2 = uint64(1000000010)
var _lookupID3 = uint64(1000000100)
var _logger = logger.New(logger.Levels.Debug)

// test some general stuff
func TestLookupsClient(t *testing.T) {
	sut, err := NewTagLookupsClient(_logger, "", "", Options{}, []string{"127.0.0.1:12345"})
	if !assert.NoError(t, err) {
		return
	}
	req := sut.NewRequest()

	// set up legacy lookups
	sut.cache.UpdateWithLegacyTagLookups(map[uint32]map[uint64]uint64{
		1013: map[uint64]uint64{
			100: 1000000010, // legacy ID 100 maps to lookup2 for companyID 1013
		},
	})

	// test bulk lookup checking
	assert.True(t, IsLookupID(100))
	assert.True(t, IsLookupID(_lookupID1))
	assert.True(t, IsLookupID(_lookupID2))

	// test adding lookup IDs
	assert.True(t, req.AddLookupID(100))
	assert.Equal(t, 1, len(req.LookupIDs))
	assert.True(t, req.AddLookupID(_lookupID1))
	assert.Equal(t, 2, len(req.LookupIDs))
	assert.Equal(t, uint64(100), req.LookupIDs[0])
	assert.Equal(t, uint64(_lookupID1), req.LookupIDs[1])
}

// test lookups on a disabled client
func TestLookupsDisabled(t *testing.T) {
	sut, err := NewTagLookupsClient(_logger, "", "", Options{Disabled: true}, []string{"127.0.0.1:12345"})
	if !assert.NoError(t, err) {
		return
	}
	request := sut.NewRequest()
	request.AddLookupID(_lookupID1)
	request.AddLookupID(_lookupID2)

	result, resultErr := sut.FetchLookups(10, request)
	assert.NotNil(t, result)
	assert.Nil(t, resultErr)
	assert.Equal(t, "", result.GetLookupValue(_lookupID1))
	assert.Equal(t, "", result.GetLookupValue(_lookupID2))
}

// test tag lookups against an active lookup server that works
func TestLookupsSuccess(t *testing.T) {
	response1 := lookups.NewTagLookupsResponse()
	response1.AddLookup(_lookupID1, "ABC")
	response1.AddLookup(_lookupID2, "DEF")
	response1.AddLookup(_lookupID3, "FOO")

	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		w.Header().Set("Content-Type", "application/protobuf; proto=com.kentik.tagstreaming.models.lookups")
		w.WriteHeader(http.StatusOK)
		w.Write(responseToBytes(response1))
	}))
	defer ts.Close()

	sut, err := NewTagLookupsClient(_logger, "", strings.Replace(ts.URL, "http://", "", -1), Options{}, []string{"127.0.0.1:12345"})
	if !assert.NoError(t, err) {
		return
	}

	// set up legacy lookups
	sut.cache.UpdateWithLegacyTagLookups(map[uint32]map[uint64]uint64{
		1013: map[uint64]uint64{
			100: 1000000010, // legacy ID 100 maps to lookup2 for companyID 1013
		},
	})

	request := sut.NewRequest()
	request.AddLookupID(100) // this one will be found, mapped to lookupID2: "DEF" via legacy lookups
	request.AddLookupID(_lookupID1)
	request.AddLookupID(_lookupID2)
	request.AddReverseLookup("FOO")
	request.AddLookupID(555) // this one won't be found

	// submit request and check response
	result, resultErr := sut.FetchLookups(1013, request)
	assert.NotNil(t, result)
	assert.Nil(t, resultErr)
	assert.Equal(t, "DEF", result.GetLookupValue(100))
	assert.Equal(t, "ABC", result.GetLookupValue(_lookupID1))
	assert.Equal(t, _lookupID1, result.GetLookupIDs("ABC").OneID())
	assert.Equal(t, "DEF", result.GetLookupValue(_lookupID2))
	assert.Equal(t, _lookupID2, result.GetLookupIDs("DEF").OneID())
	assert.Equal(t, "FOO", result.GetLookupValue(_lookupID3))
	assert.Equal(t, _lookupID3, result.GetLookupIDs("FOO").OneID())
	assert.Equal(t, "", result.GetLookupValue(555))
	assert.Equal(t, 1, requestCount)

	// try again with the same request, and make sure it's served from cache
	request = sut.NewRequest()
	request.AddLookupID(100) // this one will be found, mapped to lookupID2: "DEF" via legacy lookups
	request.AddLookupID(_lookupID1)
	request.AddLookupID(_lookupID2)
	request.AddReverseLookup("FOO")
	request.AddLookupID(555) // this one won't be found

	result, resultErr = sut.FetchLookups(1013, request)
	assert.NotNil(t, result)
	assert.Nil(t, resultErr)
	assert.Equal(t, "DEF", result.GetLookupValue(100))
	assert.Equal(t, "ABC", result.GetLookupValue(_lookupID1))
	assert.Equal(t, _lookupID1, result.GetLookupIDs("ABC").OneID())
	assert.Equal(t, "DEF", result.GetLookupValue(_lookupID2))
	assert.Equal(t, _lookupID2, result.GetLookupIDs("DEF").OneID())
	assert.Equal(t, "FOO", result.GetLookupValue(_lookupID3))
	assert.Equal(t, _lookupID3, result.GetLookupIDs("FOO").OneID())
	assert.Equal(t, 1, requestCount)
}

// test tag lookups against a server that's taking too long to respond
// TODO: This test will be finicky so long as we're relying on time.Sleep.
// 		 Instead, we can come up with a system of explicit blocking and freeing with
//       channels
func TestLookupsTimeout(t *testing.T) {
	response1 := lookups.NewTagLookupsResponse()
	response1.AddLookup(_lookupID1, "ABC")
	response1.AddLookup(_lookupID2, "DEF")

	response2 := lookups.NewTagLookupsResponse()
	response2.AddLookup(_lookupID1, "ABC")
	response2.AddLookup(_lookupID3, "GHI")
	response2.AddLookup(_lookupID2, "DEF")

	response3 := lookups.NewTagLookupsResponse()
	response3.AddLookup(_lookupID1, "ABC")
	response3.AddLookup(_lookupID3, "GHI")
	response3.AddLookup(_lookupID2, "DEF")
	response3.AddLookup(0, "FOOBAR")

	responseNumToRespond := 1

	requestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 500ms is longer than our client is willing to wait
		requestCount++

		if requestCount == 1 {
			// timeout once
			time.Sleep(500 * time.Millisecond)
		}

		// but we still return a value respond
		var responseBytes []byte
		if responseNumToRespond == 1 {
			responseBytes = responseToBytes(response1)
		} else if responseNumToRespond == 2 {
			responseBytes = responseToBytes(response2)
		} else if responseNumToRespond == 3 {
			responseBytes = responseToBytes(response3)
		} else {
			panic("unhandled responseNumToRespond")
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(responseBytes)
	}))
	defer ts.Close()

	// 150ms timeout, 500ms request, 1 second lockout
	sut, err := NewTagLookupsClient(_logger, "", strings.Replace(ts.URL, "http://", "", -1), Options{HTTPTimeoutDuration: 150 * time.Millisecond, ErrorLockoutDuration: 1 * time.Second}, []string{})
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, sut.isEnabled())

	// submit request and check response
	request := sut.NewRequest()
	request.AddLookupID(_lookupID1)
	request.AddLookupID(_lookupID2)
	start := time.Now()
	result, resultErr := sut.FetchLookups(10, request)
	assert.Nil(t, result)
	assert.NotNil(t, resultErr)
	assert.Equal(t, 1, requestCount)
	assert.True(t, time.Now().Sub(start) < 250*time.Millisecond) // added some padding

	// make sure the client is disabled
	assert.False(t, sut.isEnabled())

	// try again with the same request, and make sure it responds instantly (due to timeout)
	request = sut.NewRequest()
	request.AddLookupID(_lookupID1)
	request.AddLookupID(_lookupID2)
	start = time.Now()
	result, resultErr = sut.FetchLookups(10, request)
	// we get back no error this time - just an empty response
	assert.NotNil(t, result)
	assert.Nil(t, resultErr)
	assert.Equal(t, "", result.GetLookupValue(_lookupID1))
	assert.Equal(t, "", result.GetLookupValue(_lookupID2))
	assert.Equal(t, 1, requestCount)                             // didn't change
	assert.True(t, time.Now().Sub(start) < 100*time.Millisecond) // added some padding

	// wait longer than the timeout period
	assert.False(t, sut.isEnabled())    // disabled
	time.Sleep(1500 * time.Millisecond) // wait past disable period
	assert.True(t, sut.isEnabled())     // enabled

	// try again - this time, the server responds immediately
	request = sut.NewRequest()
	request.AddLookupID(_lookupID1)
	request.AddLookupID(_lookupID2)
	start = time.Now()
	result, resultErr = sut.FetchLookups(10, request)
	assert.NotNil(t, result)
	assert.Nil(t, resultErr)
	assert.Equal(t, "ABC", result.GetLookupValue(_lookupID1))
	assert.Equal(t, "DEF", result.GetLookupValue(_lookupID2))
	assert.Equal(t, 2, requestCount)                             // incremented
	assert.True(t, time.Now().Sub(start) < 100*time.Millisecond) // added some padding

	// and again, this time from cache
	start = time.Now()
	result, resultErr = sut.FetchLookups(10, request)
	assert.NotNil(t, result)
	assert.Nil(t, resultErr)
	assert.Equal(t, "ABC", result.GetLookupValue(_lookupID1))
	assert.Equal(t, "DEF", result.GetLookupValue(_lookupID2))
	assert.Equal(t, 2, requestCount)                             // didn't change
	assert.True(t, time.Now().Sub(start) < 100*time.Millisecond) // added some padding

	// again - this time with an extra lookup
	request = sut.NewRequest()
	request.AddLookupID(_lookupID1)
	request.AddLookupID(_lookupID2)
	request.AddLookupID(_lookupID3)
	start = time.Now()
	responseNumToRespond = 2
	result, resultErr = sut.FetchLookups(10, request)
	assert.NotNil(t, result)
	assert.Nil(t, resultErr)
	assert.Equal(t, "ABC", result.GetLookupValue(_lookupID1))
	assert.Equal(t, "DEF", result.GetLookupValue(_lookupID2))
	assert.Equal(t, "GHI", result.GetLookupValue(_lookupID3))
	assert.Equal(t, 3, requestCount)                             // incremented
	assert.True(t, time.Now().Sub(start) < 100*time.Millisecond) // added some padding

	// repeat last request - should come from cache
	request = sut.NewRequest()
	request.AddLookupID(_lookupID1)
	request.AddLookupID(_lookupID2)
	request.AddLookupID(_lookupID3)
	start = time.Now()
	responseNumToRespond = 2
	result, resultErr = sut.FetchLookups(10, request)
	assert.NotNil(t, result)
	assert.Nil(t, resultErr)
	assert.Equal(t, "ABC", result.GetLookupValue(_lookupID1))
	assert.Equal(t, "DEF", result.GetLookupValue(_lookupID2))
	assert.Equal(t, "GHI", result.GetLookupValue(_lookupID3))
	assert.Equal(t, 3, requestCount)                             // didn't change
	assert.True(t, time.Now().Sub(start) < 100*time.Millisecond) // added some padding

	// repeat last request, but with an added reverse lookup that's not found
	request = sut.NewRequest()
	request.AddLookupID(_lookupID1)
	request.AddLookupID(_lookupID2)
	request.AddLookupID(_lookupID3)
	request.AddReverseLookup("FOOBAR")
	start = time.Now()
	responseNumToRespond = 3
	result, resultErr = sut.FetchLookups(10, request)
	assert.NotNil(t, result)
	assert.Nil(t, resultErr)
	assert.Equal(t, "ABC", result.GetLookupValue(_lookupID1))
	assert.Equal(t, "DEF", result.GetLookupValue(_lookupID2))
	assert.Equal(t, "GHI", result.GetLookupValue(_lookupID3))
	assert.Equal(t, uint64(0), result.GetLookupIDs("FOOBAR").OneID())
	assert.Equal(t, 4, requestCount)                             // incremented
	assert.True(t, time.Now().Sub(start) < 100*time.Millisecond) // added some padding

	// repeat last request - make sure we get the same response
	request = sut.NewRequest()
	request.AddLookupID(_lookupID1)
	request.AddLookupID(_lookupID2)
	request.AddLookupID(_lookupID3)
	request.AddReverseLookup("FOOBAR")
	start = time.Now()
	responseNumToRespond = 3
	result, resultErr = sut.FetchLookups(10, request)
	assert.NotNil(t, result)
	assert.Nil(t, resultErr)
	assert.Equal(t, "ABC", result.GetLookupValue(_lookupID1))
	assert.Equal(t, "DEF", result.GetLookupValue(_lookupID2))
	assert.Equal(t, "GHI", result.GetLookupValue(_lookupID3))
	assert.Equal(t, uint64(0), result.GetLookupIDs("FOOBAR").OneID())
	assert.Equal(t, 4, requestCount)                             // didn't change
	assert.True(t, time.Now().Sub(start) < 100*time.Millisecond) // added some padding
}

func responseToBytes(response *lookups.TagLookupsResponse) []byte {
	resp, err := response.Marshal()
	if err != nil {
		panic("error marshalling response")
	}
	return resp
}
