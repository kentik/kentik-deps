Lookup Client Library
=====================

Bulk tags are stored in the `ch_tagging` database, with string->int lookups stored in `mn_tag_lookup`,
and managed by the `tag-ingest` service. Other services that need to perform the reverse lookups will
query the `tag-ingest` service via this package.'
