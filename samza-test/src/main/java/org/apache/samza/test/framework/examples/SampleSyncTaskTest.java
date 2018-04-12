package org.apache.samza.test.framework.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.framework.utils.StreamAssert;
import org.apache.samza.system.inmemory.InMemorySystemUtils.*;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.junit.Test;


public class SampleSyncTaskTest {

  @Test
  public void testSyncTaskWithConcurrency() throws Exception{
    // Create a sample data
    List<Integer> input = Arrays.asList(1,2,3,4,5);
    List<Integer> output = Arrays.asList(10,20,30,40,50);

    // Create a StreamTask
    StreamTask task = new StreamTask() {
      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Integer obj = (Integer)envelope.getMessage();
        collector.send(new OutgoingMessageEnvelope(new SystemStream("test","output"), obj * 10));
      }
    };


    TestRunner
        .of(task)
        .addInputStream(CollectionStream.of("test.input",input))
        .addOutputStream(CollectionStream.empty("test.output"))
        .run();

    StreamAssert.that("test.output").contains(output);

  }
}
