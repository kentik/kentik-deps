Cloud Topology: Traffic
=======================

chfclient samples bytes in/out between pairs of (currently AWS) regions, VPCs, subnets, and VMs,
publishes these samples into Kafka every minute per source entity, into the [sample](sample) subdirectory's
Kafka topic, `kentik.interface_streaming.topology.cloud.traffic.sample`.

[interface-streaming](https://github.com/kentik/interface-streaming/tree/master/cmd/interface-streaming) 
service consumes these samples, then combines them all into summaries, and publishes them into the [summary](summary)
subdirectory's Kafka topic, `kentik.interface_streaming.topology.cloud.traffic.summary`.

[interface-api](https://github.com/kentik/interface-streaming/tree/master/cmd/interface-api) service consumes
these summaries, loading them into cache, to handle API requests from the Portal.
