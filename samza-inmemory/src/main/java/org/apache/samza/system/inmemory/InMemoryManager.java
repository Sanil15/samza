/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.system.inmemory;

import java.io.IOException;
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
import org.apache.samza.system.SystemStreamMetadata;
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
    bufferedMessages.computeIfAbsent(ssp, value -> new LinkedList<>());
    List<IncomingMessageEnvelope> messages = bufferedMessages.get(ssp);
    int offset =   messages.size();
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

    // we are at head and nothing to return
    if (startingOffset >= messageEnvelopesForSSP.size()) {
      return new LinkedList<>();
    }
    return messageEnvelopesForSSP.subList(startingOffset, messageEnvelopesForSSP.size());
  }

  public boolean initializeStream(StreamSpec streamSpec, String serializedDataSet){
    try {
      Set<Object> dataSet = InMemorySystemUtils.deserialize(serializedDataSet);

    int partitionCount = streamSpec.getPartitionCount();
    int partition = 0;

    for (Object data : dataSet) {
      put(new SystemStreamPartition(streamSpec.toSystemStream(), new Partition(partition)), data);
      partition = (partition + 1) % partitionCount;
    }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return true;
  }

  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    Map<String, Map<SystemStreamPartition, LinkedList<IncomingMessageEnvelope>>> result =
        bufferedMessages.entrySet()
            .stream()
            .filter(map -> streamNames.contains(map.getKey().getStream()))
            .collect(Collectors.groupingBy(entry -> entry.getKey().getStream(),
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    return result.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> constructSystemStreamMetadata(entry.getKey(), entry.getValue())));
  }

  private SystemStreamMetadata constructSystemStreamMetadata(
      String systemName,
      Map<SystemStreamPartition, LinkedList<IncomingMessageEnvelope>> sspToMessagesForSystem) {

    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata =
        sspToMessagesForSystem
            .entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getPartition(), entry -> {
              String oldestOffset = "0";
              String newestOffset = String.valueOf(entry.getValue().size());
              String upcomingOffset = String.valueOf(entry.getValue().size() + 1);

              return new SystemStreamMetadata.SystemStreamPartitionMetadata(oldestOffset, newestOffset, upcomingOffset);

            }));

    return new SystemStreamMetadata(systemName, partitionMetadata);
  }
}
