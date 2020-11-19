package response

// Topic is the Kafka topic for this consumer and producer
var Topic = "kentik.tagging.functest.flow.response"

// Note: 128 partitions with hash partitioning
var PartitionCount = uint32(128)
