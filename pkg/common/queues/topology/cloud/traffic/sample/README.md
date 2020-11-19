Cloud Traffic Sample
====================

Kafka Topic: `kentik.interface_streaming.topology.cloud.traffic.sample`

This topic is fed by chfclients sampling traffic between cloud entities.
Currently, this is just being implemented by AWS chfclients. Each AWS
chfclient will send a message once per minute per source entity, which
might be a region, VPC, subnet, or VM instance.

Each message contains a random integer set by the producer to help the
consumer decide when its seen a message that's been submitted to Kafka
multiple times.
