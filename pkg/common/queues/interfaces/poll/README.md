# kentik.interface_streaming.interface.polled-event

This topic holds information about a device interface that was polled by SNMP

After polling SNMP, `chfclient` instances send all of the interface data received
into this Kafka topic, then send the top 500 interfaces to the Portal API, where
it's inserted into `ch_www.mn_interface`, with old values inserted into 
`ch_www.mn_interface_history`.

The `interface-streaming` service consumes these messages, determines what changed
by comparing against its local cache, then produces [kentik.interface_streaming.interface.change-event](../changed)
messages to broadcast these changes.


**Producers:** `chfclient` instances

**Consumer:** `interface-streaming` instance


**Data included in message**

In each Kafka message, [this protobuf-serialized data structure](interface_poll.proto) is included.
