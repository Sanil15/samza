package org.apache.samza.test.framework.stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.system.inmemory.InMemorySystemProducer;
import org.apache.samza.test.framework.Base64Serializer;
import org.apache.samza.test.framework.TestTask;


public class CollectionStream<T> {
  private Collection<T> collection;
  private Map<String,String> streamConfig;
  private String systemStream;
  private String systemName;
  private Integer partitionCount;

  public CollectionStream(String systemStream, List<T> collection, Integer partitionCount) {
    this.systemStream = systemStream;
    this.systemName = TestTask.systemName;
    this.streamConfig = new HashMap<>();
    this.collection = collection;
    this.partitionCount = partitionCount;
    streamConfig.put("streams."+systemStream+".samza.system", systemName);

    // Ensure task reads the input
    streamConfig.put(TaskConfig.INPUT_STREAMS(), systemName+"."+systemStream);
    // Initialize the input by spinning up a producer
    SystemProducer producer= new InMemorySystemFactory()
        .getProducer(systemName, new MapConfig(streamConfig), null);
    collection.forEach(T -> {
      producer.send(null, new OutgoingMessageEnvelope(new SystemStream(systemName,systemStream), T));
    });
    producer.send(null, new OutgoingMessageEnvelope(new SystemStream(systemName,systemStream), new EndOfStreamMessage(null)));
  }

  public CollectionStream(String systemStream) {
    this.systemStream = systemStream;
    this.systemName = TestTask.systemName;
    this.streamConfig = new HashMap<>();
    streamConfig.put("streams."+systemStream+".samza.system", systemName);
  }

  public String getSystemStream() {
    return systemStream;
  }

  public void setSystemStream(String systemStream) {
    this.systemStream = systemStream;
  }

  public String getSystemName() {
    return systemName;
  }

  public void setSystemName(String systemName) {
    this.systemName = systemName;
  }

  public Map<String, String> getStreamConfig() {
    return streamConfig;
  }

  public void setStreamConfig(Map<String, String> streamConfig) {
    this.streamConfig = streamConfig;
  }

  public static <T> CollectionStream<T> empty(String systemStream) {
    return new CollectionStream<>(systemStream);
  }

  public static <T> CollectionStream<T> of(String systemStream, List<T> collection){
    return new CollectionStream<>(systemStream, collection, 1);
  }

  public static <K, V> CollectionStream<KV<K, V>> of(String systemStream, Map<K, V> elems) {
    List<KV<K, V>> kvs = new ArrayList<>(elems.size());
    for (Map.Entry<K, V> entry : elems.entrySet()) {
      kvs.add(KV.of(entry.getKey(), entry.getValue()));
    }
    return of(systemStream, kvs);
  }
}
