package org.apache.samza.test.framework.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.framework.utils.StreamAssert;
import org.apache.samza.system.inmemory.InMemorySystemUtils.*;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.Mode;
import org.apache.samza.test.framework.TestTask;
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
        collector.send(new OutgoingMessageEnvelope(new SystemStream("test","Output"), obj * 10));
      }
    };

    // Run the test framework
    TestTask
        .create(task, new HashMap<>(), Mode.SINGLE_CONTAINER)
        .setJobContainerThreadPoolSize(4)
        .setTaskMaxConcurrency(4)
        .addInputStream(CollectionStream.of("test.SomeView", input))
        .addOutputStream(CollectionStream.empty("test.Output"))
        .run();

    StreamAssert.that("test.Output").containsInAnyOrder(output);
  }
}
