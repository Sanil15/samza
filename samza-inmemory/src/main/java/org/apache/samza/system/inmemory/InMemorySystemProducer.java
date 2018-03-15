package org.apache.samza.system.inmemory;

import java.util.Optional;
import org.apache.samza.Partition;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamPartition;


/**
 *
 */
public class InMemorySystemProducer implements SystemProducer {
  private final InMemoryManager memoryManager;

  public InMemorySystemProducer(InMemoryManager manager) {
    memoryManager = manager;
  }

  /**
   * Start the SystemProducer. After this method finishes it should be ready to accept messages received from the send method.
   */
  @Override
  public void start() {

  }

  /**
   * Stop the SystemProducer. After this method finished, the system should have completed all necessary work, sent
   * any remaining messages and will not receive any new calls to the send method.
   */
  @Override
  public void stop() {

  }

  /**
   * Registers this producer to send messages from a specified Samza source, such as a StreamTask.

   * @param source String representing the source of the message.
   */
  @Override
  public void register(String source) {

  }

  /**
   * Sends a specified message envelope from a specified Samza source.

   * @param source String representing the source of the message.
   * @param envelope Aggregate object representing the serialized message to send from the source.
   */
  @Override
  public void send(String source, OutgoingMessageEnvelope envelope) {
    Object message = envelope.getMessage();
    int partition = Optional.ofNullable(envelope.getPartitionKey())
        .map(Object::hashCode)
        .orElse(0);
    SystemStreamPartition ssp = new SystemStreamPartition(envelope.getSystemStream(), new Partition(partition));

    memoryManager.put(ssp, message);
  }

  /**
   * If the SystemProducer buffers messages before sending them to its underlying system, it should flush those
   * messages and leave no messages remaining to be sent.
   *

   * @param source String representing the source of the message.
   */
  @Override
  public void flush(String source) {
    // nothing to do
  }
}
