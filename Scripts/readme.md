# The disaster recovery script ( kafka-cluster-sync.sh )
Shell script which needs to be run on kafka-source image. 

It syncs groupId, topics, and partition from source-kafka to target-kafka. It deletes existing data!

KafkaSyncProducer can be used to generate sample messages to source-kafka.

KafkaSyncConsumer can be used to verify both kafka clusters if partions/topics were synced correctly.

# The disaster recovery script ( kafka-cluster-sync.ps1 )
Same as Shell script, but can be run on Windows machine. 

It requires https://kafka.apache.org/downloads (for bin/windows scripts) on the host.
