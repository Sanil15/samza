package org.apache.samza.test.framework;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javafx.util.Pair;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.inmemory.InMemorySystemConsumer;
import org.apache.samza.system.inmemory.InMemorySystemFactory;


public class InMemoryCollectionStreamSystem {
  private static final String SYSTEM_FACTORY = "systems.%s.samza.factory";
  private static final String SYSTEM_OFFSET = "systems.%s.default.stream.samza.offset.default";
  private static final String STREAM_TO_SYSTEM = "streams.%s.samza.system";

  // Name of the System
  private String name;

  // Maintain the global job config
  private Map<String, String> systemConfigs;

  // InMemorySystemFactory
  private InMemorySystemFactory factoryTest;

  private InMemoryCollectionStreamSystem(String name){
    this.name = name;
    factoryTest = new InMemorySystemFactory();
    systemConfigs = new HashMap<String,String>();
    // System Factory InMemory System
    systemConfigs.putIfAbsent(String.format(SYSTEM_FACTORY,name), InMemorySystemFactory.class.getName());
    // Consume from the oldest for all streams in the system
    systemConfigs.putIfAbsent(String.format(SYSTEM_OFFSET,name), "oldest");
  }

  public String getName() {
    return name;
  }

  public Map<String,String> getSystemConfigs(){
    return systemConfigs;
  }

  public static InMemoryCollectionStreamSystem create(String name){
    Preconditions.checkState(name !=null);
    return new InMemoryCollectionStreamSystem(name);
  }

  public InMemoryCollectionStreamSystem addOutput(String steamId) {
    Preconditions.checkNotNull(steamId);
    systemConfigs.put(String.format(STREAM_TO_SYSTEM, steamId), name);
    return this;
  }

  public <T> InMemoryCollectionStreamSystem addInput(String systemName, String streamId, Map<Integer,? extends Iterable> partitions) {
    Preconditions.checkState(streamId != null);
    Preconditions.checkState(name != null);

    SystemProducer producer = new InMemorySystemFactory().getProducer(systemName, new MapConfig(systemConfigs), null);
    partitions.forEach((partitionId, partition) -> {
      partition.forEach(e -> {
        Object key = e instanceof Pair ? ((Pair) e).getKey() : null;
        Object value = e instanceof Pair ? ((Pair) e).getValue() : e;
        producer.send(systemName,
            new OutgoingMessageEnvelope(new SystemStream(systemName, streamId), Integer.valueOf(partitionId), key, value));

      });
    });
    return this;
  }

  public <T> List<T> getStreamState(String streamId) throws InterruptedException {

    Set<SystemStreamPartition> ssps = new HashSet<>();
    Set<String> streamNames = new HashSet<>();
    streamNames.add(streamId);
    Map<String, SystemStreamMetadata> metadata =
        factoryTest.getAdmin(name, new MapConfig()).getSystemStreamMetadata(streamNames);
    InMemorySystemConsumer consumer =
        (InMemorySystemConsumer) factoryTest.getConsumer(name, new MapConfig(new HashMap<>()), null);
    metadata.get(streamId).getSystemStreamPartitionMetadata().keySet().forEach(partition -> {
      SystemStreamPartition temp = new SystemStreamPartition(name, streamId, partition);
      ssps.add(temp);
      consumer.register(temp, "0");
    });
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> output = consumer.poll(ssps, 10);
    return output.values()
        .stream()
        .flatMap(List::stream)
        .map(e -> (T) e.getMessage())
        .filter(e -> !(e instanceof EndOfStreamMessage))
        .collect(Collectors.toList());
  }

}

