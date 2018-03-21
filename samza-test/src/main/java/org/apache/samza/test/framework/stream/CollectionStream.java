package org.apache.samza.test.framework.stream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.Serializer;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.test.framework.Base64Serializer;
import org.apache.samza.test.framework.TestStreamTask;

public class CollectionStream<T> {
  private Collection<T> collection;
  private Map<String,String> streamConfig;
  private String systemStream;
  private String systemName;
  private Integer partitionCount;

  public CollectionStream(String systemStream, List<T> collection, Integer partitionCount) {
    this.systemStream = systemStream;
    this.systemName = TestStreamTask.SYSTEM_NAME;
    this.streamConfig = new HashMap<>();
    this.collection = collection;
    this.partitionCount = partitionCount;
    streamConfig.put("streams."+systemStream+".samza.system", systemStream);
    streamConfig.put("streams."+systemStream+".dataset", Base64Serializer.serializeUnchecked(collection)); // THIS HAS TO BE CHANGED, FEED DIRECTLY
    streamConfig.put("streams."+systemStream+".partitionCount", String.valueOf(partitionCount));
    // Ensure task reads the input
    streamConfig.put(TaskConfig.INPUT_STREAMS(), systemName+"."+systemStream);
    // Initialize the input
    StreamSpec spec = new StreamSpec(this.systemStream, this.systemStream, this.systemName, this.partitionCount);
    InMemorySystemFactory factory = new InMemorySystemFactory();
    factory.getAdmin(this.systemName, new MapConfig(streamConfig)).createStream(spec);
  }

  public CollectionStream(String systemStream) {
    this.systemStream = systemStream;
    this.systemName = TestStreamTask.SYSTEM_NAME;
    this.streamConfig = new HashMap<>();
    streamConfig.put("streams."+systemStream+".samza.system", systemName);
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
}
