package kafka.deduplication.sequence;

import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

@Slf4j
public class SequenceDeduplicator {

  private static final String SEQUENCE_STORE = "sequenceStore";

  public static void main(String[] args) {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "sequence-deduplicator");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/sequence-deduplicator-stream");
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    StreamsBuilder builder = new StreamsBuilder();

    final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(SEQUENCE_STORE),
      Serdes.String(),
      Serdes.Long()
    );

    builder.addStateStore(storeBuilder);

    builder
      .stream("inSequenceTopic", Consumed.with(Serdes.String(), Serdes.String()))
      .transformValues(() -> new Transformer(SEQUENCE_STORE), SEQUENCE_STORE)
      .filter((k, v) -> v != null)
      .peek((k, v) -> log.info("Sequence Number: " + v))
      .to("outSequenceTopic", Produced.with(Serdes.String(), Serdes.Long()));

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
