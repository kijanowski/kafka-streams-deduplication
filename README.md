
## Bootstrap
```
$ ./bin/zookeeper-server-start etc/kafka/zookeeper.properties
$ ./bin/kafka-server-start etc/kafka/server.properties
```

## SequenceDeduplicator
```
$ ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic inSequenceTopic
$ ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic outSequenceTopic

$ ./bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic outSequenceTopic --from-beginning \
                               --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
$ ./bin/kafka-console-producer --broker-list localhost:9092 --topic inSequenceTopic 
```

## GroupedSequenceDeduplicator
```
$ ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic inGroupedSequenceTopic
$ ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic outGroupedSequenceTopic

$ ./bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic outGroupedSequenceTopic --from-beginning \
                               --property print.key=true

$ ./bin/kafka-console-producer --broker-list localhost:9092 --topic inGroupedSequenceTopic \
                             --property parse.key=true --property key.separator=:
```

## UuidSequenceDeduplicator
```
$ ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic inUuidTopic
$ ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic outUuidTopic

$ ./bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic outUuidTopic --from-beginning

$ ./bin/kafka-console-producer --broker-list localhost:9092 --topic inUuidTopic
$ ./bin/kafka-console-producer --broker-list localhost:9092 --topic inUuidTopic \
                               --property parse.key=true --property key.separator=:
```