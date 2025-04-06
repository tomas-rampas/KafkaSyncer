# The disaster recovery script ( kafka-cluster-sync.sh )
Shell script which needs to be run on kafka-source image. 
It syncs groupId, topics, and partition from source-kafka to target-kafka. It deletes existing data!
KafkaSyncProducer can be used to generate sample messages to source-kafka.
KafkaSyncConsumer can be used to verify both kafka clusters if partions/topics were synced correctly.

