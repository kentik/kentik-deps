package changed

// Topic is the Kafka topic for this consumer and producer
var Topic = "kentik.interface_streaming.interface.change-event"

// PartitionCount holds how many partitions we have for this topic
var PartitionCount = uint32(128)
