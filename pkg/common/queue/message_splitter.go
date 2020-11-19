package queue

import (
	uuid "github.com/google/uuid"
)

// this is how big we chunk messages
var _kafkaMessageChunkByteCount = 500000

// SplitMessage splits serialized data into an array of MessageParts
func SplitMessage(serializedData []byte) []MessagePart {
	partCount := uint32(len(serializedData) / _kafkaMessageChunkByteCount)
	if len(serializedData)%_kafkaMessageChunkByteCount > 0 {
		partCount++
	}

	guid := uuid.New().String()
	partNumber := uint32(1)

	ret := make([]MessagePart, 0, partCount)
	remainingBytes := serializedData
	for len(remainingBytes) > 0 {
		chunkLen := _kafkaMessageChunkByteCount
		if len(remainingBytes) < chunkLen {
			chunkLen = len(remainingBytes)
		}

		ret = append(ret, MessagePart{
			GUID:       guid,
			PartNumber: partNumber,
			PartCount:  partCount,
			Data:       remainingBytes[0:chunkLen],
		})
		partNumber++
		remainingBytes = remainingBytes[chunkLen:]
	}

	return ret
}
