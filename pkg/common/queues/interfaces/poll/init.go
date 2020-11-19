package poll

// Topic is the Kafka topic for this consumer and producer
var Topic = "kentik.interface_streaming.interface.poll"

// PartitionCount holds how many partitions we have for this topic
var PartitionCount = uint32(128)
