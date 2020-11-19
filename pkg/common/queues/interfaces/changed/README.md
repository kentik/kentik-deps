# kentik.interface_streaming.interface.change-event

This topic holds interface change events, as determined by `interface-streaming`

The `interface-streaming` service determines when an interface changes, based on changes
it finds in the Postgres tables `ch_www.mn_interface` and `ch_www.mn_interface_history`,
as well as what it finds in the [kentik.interface_streaming.interface.polled-event](../poll)
Kafka topic. It then pushes those changes into this Kafka topic. The topic is not only 
the delivery pipeline, but also the data store, using a key/value strategy.


**Producers:** `interface-streaming` instance

**Consumers:** `runner-master` instances, for their caches

**Key for current interfaces:**

    <companyID>-<deviceID>-<snmpID>-current


**Key for historical interfaces:**

    <companyID>-<deviceID>-<snmpID>-<start-time-in-unix-epoch-nanoseconds>


**Data included in message**

In each Kafka message, [this protobuf-serialized data structure](interfaces.proto) is included.
