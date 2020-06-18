# Burrow Prometheus Exporter

Prometheus exporter for [burrow](https://github.com/linkedin/Burrow) (Kafka consumers lag checking tool).

Exposed metrics:

* `kafka_topic_partition_offset` : 
    * exposes the latest offset committed on topic partitions.
    * labels:  `cluster`, `topic`, `partition`.
* `kafka_consumer_total_lag` : 
    * exposes consumer group total lag.
    * labels:  `cluster`, `consumer_group`. 
* `kafka_consumer_max_lag` : 
    * exposes consumer group maximum lag.
    * labels:  `cluster`, `consumer_group`, `topic`, `partition`. 
* `kafka_consumer_partition_lag` : 
    * exposes consumer current lag per topic partition.
    * labels:  `cluster`, `consumer_group`, `topic`, `partition`. 
* `kafka_consumer_partition_offset` : 
    * exposes the latest committed offset on topic partitions per group consumer.
    * labels:  `cluster`, `consumer_group`, `topic`, `partition`.   
* `kafka_consumer_partition_status` : 
    * exposes the status of topic partitions per consumer group.
    * labels:  `cluster`, `consumer_group`, `topic`, `partition`.   
* `kafka_consumer_status` : 
    * exposes the status of consumer groups.
    * labels:  `cluster`, `consumer_group`.
