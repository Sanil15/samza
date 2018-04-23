package org.apache.samza.test.framework.stream;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.serializers.Serde;
import org.apache.samza.test.framework.InMemoryCollectionStreamSystem;
import org.apache.samza.test.framework.TestRunner;
import scala.concurrent.TaskRunner;


public class CollectionStream<T>{

  private String streamId;
  private String systemName;
  private Map<Integer, Iterable<? extends T>> partitions;

  // Configs only specific to the stream
  private Map<String, String> streamConfig;

  // Serdes for the Stream
  private Serde keySerde;
  private Serde msgSerde;

  // Configs specific to streams
  private static final String STREAM_TO_SYSTEM = "streams.%s.samza.system";
  private static final String KEY_SERDE = "streams.%s.samza.key.serde";
  private static final String MSG_SERDE = "streams.%s.samza.msg.serde";


  private CollectionStream(String systemName, String streamId) {
    this.streamId = streamId;
    this.streamConfig = new HashMap<>();
    this.systemName = systemName;
    // Config Specific to stream
    streamConfig.put(String.format(STREAM_TO_SYSTEM, this.streamId), systemName);
  }

  private CollectionStream(String systemName, String streamId, Iterable<T> collection) {
    this.streamId = streamId;
    this.streamConfig = new HashMap<>();
    this.systemName = systemName;
    partitions = new HashMap<>();
    partitions.put(0, collection);

    // Config Specific to stream
    streamConfig.put(String.format(STREAM_TO_SYSTEM, this.streamId), systemName);
    streamConfig.put(TaskConfig.INPUT_STREAMS(), systemName+"."+streamId);

  }

  public Map<Integer, Iterable<? extends T>> getInitPartitions(){
    return partitions;
  }

  public String getSystemName() {
    return systemName;
  }

  public void setSystemName(String systemName) {
    this.systemName = systemName;
  }

  public String getStreamId() {
    return streamId;
  }

  public Map<String, String> getStreamConfig() {
    return streamConfig;
  }

  public CollectionStream withKeySerde(Serde serde){
    this.keySerde = serde;
    return this;
  }

  public CollectionStream withMsgSerde(Serde serde){
    this.msgSerde = serde;
    return this;
  }

  public <T> List<T> getStreamState(){
    InMemoryCollectionStreamSystem system = TestRunner.getOrIntializeInMemoryCollectionStreamSystem(systemName);
    try {
      return system.getStreamState(streamId);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static <T> CollectionStream<T> empty(String systemName, String streamId) {
    return new CollectionStream<>(systemName, streamId);
  }

  public static <T> CollectionStream<T> of(String systemName, String streamId, Iterable<T> collection) {
    return new CollectionStream<>(systemName, streamId, collection);
  }

}
