package kafka.deduplication.uuid;

import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Based on
 * https://github.com/davidandcode/hackathon_kafka/blob/master/src/test/java/io/confluent/examples/streams/EventDeduplicationLambdaIntegrationTest.java
 */
@Slf4j
public class Transformer implements ValueTransformer<String, String> {

  private final long maintainDurationMs = TimeUnit.MINUTES.toMillis(5);
  private ProcessorContext context;
  private KeyValueStore<String, Long> eventIdStore; // UUID, last seen (timestamp)
  private final String storeName;

  Transformer(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    eventIdStore = (KeyValueStore) context.getStateStore(storeName);
    this.context.schedule(
      TimeUnit.MINUTES.toMillis(1),
      PunctuationType.WALL_CLOCK_TIME,
      new Punctuator() {
        @Override
        public void punctuate(long currentStreamTimeMs) {
          log.info("Punctuating");
          try (final KeyValueIterator<String, Long> iterator = eventIdStore.all()) {
            while (iterator.hasNext()) {
              final KeyValue<String, Long> entry = iterator.next();
              final long eventTimestamp = entry.value;
              if (hasExpired(eventTimestamp, currentStreamTimeMs)) {
                log.info("Expiring key {}", entry.key);
                eventIdStore.delete(entry.key);
              }
            }
          }
        }

        private boolean hasExpired(final long eventTimestamp, final long currentStreamTimeMs) {
          return (currentStreamTimeMs - eventTimestamp) > maintainDurationMs;
        }
      }
    );
  }

  @Override
  public String transform(String value) {
    if (eventIdStore.get(value) != null) {
      // duplicate
      return null; //discard the record
    } else {
      eventIdStore.put(value, context.timestamp());
      return value;
    }
  }

  @Override
  public void close() {

  }
}
