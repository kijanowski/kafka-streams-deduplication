package kafka.deduplication.grouped_sequence;

import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class GroupedSequenceDeduplicator {

  private static final String GROUPED_SEQUENCE_STORE = "groupedSequenceStore";

  public static void main(String[] args) {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "grouped-sequence-deduplicator");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/grouped-sequence-deduplicator-stream");
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    StreamsBuilder builder = new StreamsBuilder();

    builder
      .stream("inGroupedSequenceTopic", Consumed.with(Serdes.String(), Serdes.String()))
      .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
      .aggregate(
        () -> "0;0",
        (key, value, aggregate) -> {
          log.info(">>>>>>>>> Processing message {}", value);
          String[] pair = aggregate.split(";");
          var lastElement = Long.valueOf(pair[1]);

          return Long.valueOf(value) > lastElement ?
            lastElement + ";" + value :
            lastElement + ";" + lastElement;
        },
        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(GROUPED_SEQUENCE_STORE).withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
      )
      .toStream()
      .filter((k, v) -> {
        String[] pair = v.split(";");
        return !pair[0].equals(pair[1]);
      })
      .mapValues((k, v) -> v.split(";")[1])
      .to("outGroupedSequenceTopic", Produced.with(Serdes.String(), Serdes.String()));

    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        streams.close();
        log.info("Stream stopped");
      } catch (Exception exc) {
        log.error("Got exception while executing shutdown hook: ", exc);
      }
    }));

  }
}
