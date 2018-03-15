package org.apache.samza.test.framework;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;


public class TestStreamTask implements StreamTask {
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    Map<String,Object> outgoing = new HashMap<>();
    System.out.println("----------------------------------------"+envelope.getKey()+"-----------------------------");
    outgoing.put((String) envelope.getKey(),envelope.getMessage());
    collector.send(new OutgoingMessageEnvelope(new SystemStream("test","Output"), outgoing));
  }
}
