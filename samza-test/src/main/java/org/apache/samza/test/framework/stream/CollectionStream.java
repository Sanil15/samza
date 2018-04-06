package org.apache.samza.test.framework.stream;

import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.test.framework.TestTask;


public class CollectionStream<T> {
  private Map<String, String> streamConfig;
  private String streamId;
  private String systemName;
  private Integer partitionCount;
  private static final String STREAM_TO_SYSTEM = "streams.%s.samza.system";

  private CollectionStream(String streamId, Iterable<T> collection, Integer partitionCount) {
    Preconditions.checkState(streamId.indexOf(".") > 0 && streamId.indexOf(".") < streamId.length() - 1);
    this.streamId = streamId.substring(streamId.indexOf(".") + 1);
    this.systemName = streamId.substring(0, streamId.indexOf("."));
    this.streamConfig = new HashMap<>();
    this.partitionCount = partitionCount;

    // Config Specific to strean
    streamConfig.put(String.format(STREAM_TO_SYSTEM, this.streamId), systemName);
    streamConfig.put(TaskConfig.INPUT_STREAMS(), streamId);

    // Initialize the input by spinning up a producer
    SystemProducer producer = new InMemorySystemFactory().getProducer(systemName, new MapConfig(streamConfig), null);
    collection.forEach(T -> {
      Object key = T instanceof Map.Entry ? ((Map.Entry) T).getKey() : null;
      Object value = T instanceof Map.Entry ? ((Map.Entry) T).getValue() : T;
      producer.send(this.streamId,
          new OutgoingMessageEnvelope(new SystemStream(systemName, this.streamId), "0", key, value));
    });
  }

  private CollectionStream(String streamId) {
    Preconditions.checkState(streamId.indexOf(".") > 0 && streamId.indexOf(".") < streamId.length() - 1);
    this.streamId = streamId.substring(streamId.indexOf(".") + 1);
    this.systemName = streamId.substring(0, streamId.indexOf("."));
    this.streamConfig = new HashMap<>();
  }

  private CollectionStream(String streamId, Map<Integer,? extends Iterable> collection) {
    Preconditions.checkState(streamId.indexOf(".") > 0 && streamId.indexOf(".") < streamId.length() - 1);
    this.streamId = streamId.substring(streamId.indexOf(".") + 1);
    this.systemName = streamId.substring(0, streamId.indexOf("."));

    this.streamConfig = new HashMap<>();
    this.partitionCount = collection.size();

    // Config Specific to stream
    streamConfig.put(String.format(STREAM_TO_SYSTEM, this.streamId), systemName);
    streamConfig.put(TaskConfig.INPUT_STREAMS(), streamId);

    // Initialize the input by spinning up a producer
    SystemProducer producer = new InMemorySystemFactory().getProducer(systemName, new MapConfig(streamConfig), null);

    collection.forEach((partitionId, partition) -> {
      partition.forEach(e -> {
          Object key = e instanceof Pair ? ((Pair) e).getKey() : null;
          Object value = e instanceof Pair ? ((Pair) e).getValue() : e;
        producer.send(systemName,
            new OutgoingMessageEnvelope(new SystemStream(systemName, this.streamId), Integer.valueOf(partitionId), key, value));

      });
    });
  }

  public String getSystemName() {
    return systemName;
  }

  public String getStreamId() {
    return systemName+"."+streamId;
  }

  public void setSystemName(String systemName) {
    this.systemName = systemName;
  }

  public Map<String, String> getStreamConfig() {
    return streamConfig;
  }

  public static <T> CollectionStream<T> empty(String streamId) {
    return new CollectionStream<>(streamId);
  }

  public static <T> CollectionStream<T> of(String streamId, Iterable<T> collection) {
    return new CollectionStream<>(streamId, collection, 1);
  }

  public static <T> CollectionStream<T> of(String streamId, Map<Integer,? extends Iterable> partitions) {
    return new CollectionStream<>(streamId, partitions);
  }

}
