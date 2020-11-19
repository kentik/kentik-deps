Cloud Hierarchy
===============

Kafka Topic: `kentik.interface_streaming.topology.cloud.hierarchy`

This topic is fed by chfclients finding cloud hierarchies in tagged flow. Later,
we might feed this with daemons that poll cloud APIs. These messages will have a
key for de-duplication, and an expiry that lasts as long as we want to consider
these hierarchies active.
