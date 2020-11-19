# kentik.interface_streaming.device.change-event

This topic holds device change events, as determined by `interface-streaming`

**Producers:** `interface-streaming` instance

**Consumers:** `runner-master` instances, for their caches

**Key for current interfaces:**

    device-<companyID>-<deviceID>

**Data included in message**

In each Kafka message, [this protobuf-serialized data structure](device.proto) is included.
