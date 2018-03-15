package org.apache.samza.system.inmemory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStreamPartition;


/**
 *
 */
public class InMemoryManager {
  private final ConcurrentHashMap<SystemStreamPartition, LinkedList<IncomingMessageEnvelope>> bufferedMessages;

  public InMemoryManager() {
    bufferedMessages = new ConcurrentHashMap<>();
  }

  public void register(SystemStreamPartition ssp, String offset) {
    bufferedMessages.putIfAbsent(ssp, newSynchronizedLinkedList());
  }

  private LinkedList<IncomingMessageEnvelope> newSynchronizedLinkedList() {
    return (LinkedList<IncomingMessageEnvelope>) Collections.synchronizedList(new LinkedList<IncomingMessageEnvelope>());
  }

  public void put(SystemStreamPartition ssp, Object message) {
    List<IncomingMessageEnvelope> messages = bufferedMessages.get(ssp);
    int offset = messages.size() + 1;
    IncomingMessageEnvelope messageEnvelope = new IncomingMessageEnvelope(ssp, String.valueOf(offset), null, message);
    bufferedMessages.get(ssp)
        .addLast(messageEnvelope);
  }

  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Map<SystemStreamPartition, String> sspsToOffsets) {
    return sspsToOffsets.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> poll(entry.getKey(), entry.getValue())));
  }

  private List<IncomingMessageEnvelope> poll(SystemStreamPartition ssp, String offset) {
    int startingOffset = Integer.parseInt(offset);
    List<IncomingMessageEnvelope> messageEnvelopesForSSP = bufferedMessages.getOrDefault(ssp, new LinkedList<>());

    return messageEnvelopesForSSP.subList(startingOffset, messageEnvelopesForSSP.size());
  }

  public boolean initializeStream(StreamSpec streamSpec, String serializedDataSet) {
    Set<Object> dataSet = InMemorySystemUtils.deserialize(serializedDataSet);
    int partitionCount = streamSpec.getPartitionCount();
    int partition = 0;

    for (Object data : dataSet) {
      put(new SystemStreamPartition(streamSpec.toSystemStream(), new Partition(partition)), data);
      partition = (partition + 1) % partitionCount;
    }

    return true;
  }
}
