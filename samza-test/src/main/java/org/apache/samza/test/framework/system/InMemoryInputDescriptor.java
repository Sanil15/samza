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

package org.apache.samza.test.framework.system;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.serializers.NoOpSerde;

/**
 * A descriptor for an in memory stream of messages that can either have single or multiple partitions.
 * <p>
 *  An instance of this descriptor may be obtained from an appropriately configured {@link InMemorySystemDescriptor}.
 * <p>
 * @param <StreamMessageType> type of messages in input stream
 */
public class InMemoryInputDescriptor<StreamMessageType>
    extends InputDescriptor<StreamMessageType, InMemoryInputDescriptor<StreamMessageType>> {

  private Map<Integer, Iterable<StreamMessageType>> partitionData = new HashMap<Integer, Iterable<StreamMessageType>>();

  /**
   * Constructs a new InMemoryInputDescriptor from specified components.
   * @param systemDescriptor represents name of the system stream is associated with
   * @param streamId represents name of the stream
   */
  InMemoryInputDescriptor(String streamId, InMemorySystemDescriptor systemDescriptor) {
    super(streamId, new NoOpSerde<>(), systemDescriptor, null);
  }

  /**
   * Creates a single partitioned in memory stream with the provided messages
   * @param messages messages used to initialize the single partition stream
   * @return this input descriptor
   * <p>
   * {@code StreamMessageType} here can represent a message with null key or a KV {@link org.apache.samza.operators.KV}.
   * A key of which represents key of {@link org.apache.samza.system.IncomingMessageEnvelope} or
   * {@link org.apache.samza.system.OutgoingMessageEnvelope} and value represents the message of the same
   * <p>
   */
  public InMemoryInputDescriptor<StreamMessageType> withData(List<StreamMessageType> messages) {
    Preconditions.checkNotNull(messages);
    this.partitionData.put(0, messages);
    return this;
  }

  /**
   * Creates a single partitioned in memory stream with the provided messages
   *
   * @param messages key of the map represents partitionId and value represents
   *                      messages in the partition
   * @return this input descriptor
   * <p>
   * {@code StreamMessageType} here can represent a message with null key or a KV {@link org.apache.samza.operators.KV}.
   * A key of which represents key of {@link org.apache.samza.system.IncomingMessageEnvelope} or
   * {@link org.apache.samza.system.OutgoingMessageEnvelope} and value represents the message of the same
   * <p>
   */
  public InMemoryInputDescriptor<StreamMessageType> withData(
      Map<Integer, ? extends Iterable<StreamMessageType>> messages) {
    Preconditions.checkNotNull(messages);
    this.partitionData.putAll(messages);
    return this;
  }

  public int getPartitionSize() {
    return partitionData.size();
  }

  public Map<Integer, Iterable<StreamMessageType>> getPartitionData() {
    return partitionData;
  }
}
