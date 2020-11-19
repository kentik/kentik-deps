Cloud Traffic Summary
====================

Kafka Topic: `kentik.interface_streaming.topology.cloud.traffic.summary`

This topic is fed by 
[interface-streaming](https://github.com/kentik/interface-streaming/tree/master/cmd/interface-streaming),
which combines traffic samples from different cloud chfclient instances. Each message represents
a 3-hour window of combined traffic samples.
