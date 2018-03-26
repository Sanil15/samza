package org.apache.samza.test.framework;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.inmemory.InMemorySystemConsumer;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;


public class TaskAssert<T> {
  private String systemStream;
  private String systemName;


  public TaskAssert(String systemName, String systemStream) {
    this.systemStream = systemStream;
    this.systemName = systemName;
  }

  public static <T> TaskAssert<T> that(String systemName, String systemStream) {
    return new TaskAssert<>(systemName, systemStream);
  }


  public List<T> consume() throws InterruptedException{
    InMemorySystemFactory factory = new InMemorySystemFactory();
    Set<SystemStreamPartition> ssps = new HashSet<>();
    Set<String> streamNames = new HashSet<>();
    streamNames.add(systemStream);
    Map<String,SystemStreamMetadata> metadata= factory.getAdmin(systemName, new MapConfig()).getSystemStreamMetadata(streamNames);
    InMemorySystemConsumer consumer= (InMemorySystemConsumer) factory.getConsumer(systemName,  new MapConfig(new HashMap<>()), null);
    metadata.get(systemStream).getSystemStreamPartitionMetadata().keySet().forEach(partition -> {
      SystemStreamPartition temp = new SystemStreamPartition(systemName, systemStream, partition);
      ssps.add(temp);
      consumer.register(temp, null);
    });
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> output = consumer.poll(ssps, 10);
    return output
        .values()
        .stream()
        .flatMap(List::stream)
        .map(e -> {
          return (T)e.getMessage();
        })
        .collect(Collectors.toList());
  }

  public void containsInAnyOrder(List<? extends Object> expected) throws InterruptedException {
    assertThat(consume(), IsIterableContainingInAnyOrder.containsInAnyOrder(expected.toArray()));
  }

  public void contains(List<? extends Object> expected) throws InterruptedException {
    assertThat(consume(), is(expected.toArray()));
  }

}
