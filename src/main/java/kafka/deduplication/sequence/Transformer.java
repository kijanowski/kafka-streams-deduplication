package kafka.deduplication.sequence;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class Transformer implements ValueTransformer<String, Long> {

  private static final String KEY = "key";

  private final String storeName;
  private KeyValueStore<String, Long> store;

  Transformer(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public void init(ProcessorContext context) {
    store = (KeyValueStore) context.getStateStore(storeName);
  }

  @Override
  public Long transform(String value) {
    Long previous = readFromStore();
    Long current = Long.valueOf(value);
    return current > previous ?
      saveToStore(current) :
      null;
  }

  @Override
  public void close() {
  }

  private Long readFromStore() {
    Long previous = store.get(KEY);
    return previous == null ?
      Long.MIN_VALUE :
      previous;
  }

  private Long saveToStore(Long value) {
    store.put(KEY, value);
    return value;
  }
}
