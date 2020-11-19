# Smoke Test: Flow Response

A message in this topic contains a test flow record that's been received
via the `testflow` topic and tagged by a tagging client in chfclient.

Since each chfclient writes to this topic, there are 128 Kafka partitions,
with hashcode partitioning.
