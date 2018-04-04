package org.apache.samza.test.framework.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.system.framework.utils.TaskAssert;
import org.apache.samza.test.framework.Mode;
import org.apache.samza.test.framework.TestTask;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.junit.Test;


public class SampleSyncMultiPartitionTest {

  @Test
  public void testSampleSyncMultiPartitionTestWithConcurrency() throws Exception {
    // Create a sample data
    List<List<Integer>> list = new ArrayList<>();
    List<List<Integer>> expected = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      list.add(new ArrayList<Integer>());
      expected.add(new ArrayList<Integer>());
      for (int j = 0; j < 4; j++) {
        list.get(i).add(j);
        expected.get(i).add(j * 10);
      }
    }

    // Create a StreamTask
    StreamTask task = new StreamTask() {
      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
          throws Exception {
        Integer obj = (Integer) envelope.getMessage();
        collector.send(new OutgoingMessageEnvelope(new SystemStream("test", "Output"),
            Integer.valueOf(envelope.getSystemStreamPartition().getPartition().getPartitionId()), envelope.getKey(),
            obj * 10));
      }
    };

    // Run the test framework
    TestTask
        .create(task, new HashMap<>(), Mode.SINGLE_CONTAINER)
        .setJobContainerThreadPoolSize(4)
        .setTaskMaxConcurrency(2)
        .addInputStream(CollectionStream.ofPartitions("test.Integer", list))
        .addOutputStream(CollectionStream.empty("test.Output"))
        .run();

    TaskAssert.that("test.Output").comparePartitionsInAnyOrder(expected);
  }

  @Test
  public void testSampleSyncMultiPartitionTest() throws Exception {
    // Create a sample data
    List<List<Integer>> list = new ArrayList<>();
    List<List<Integer>> expected = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      list.add(new ArrayList<Integer>());
      expected.add(new ArrayList<Integer>());
      for (int j = 0; j < 4; j++) {
        list.get(i).add(j);
        expected.get(i).add(j * 10);
      }
    }

    // Create a StreamTask
    StreamTask task = new StreamTask() {
      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
          throws Exception {
        Integer obj = (Integer) envelope.getMessage();
        collector.send(new OutgoingMessageEnvelope(new SystemStream("testOrder", "OutputOrder"),
            Integer.valueOf(envelope.getSystemStreamPartition().getPartition().getPartitionId()), envelope.getKey(),
            obj * 10));
      }
    };

    // Run the test framework
    TestTask
        .create(task, new HashMap<>(), Mode.SINGLE_CONTAINER)
        .addInputStream(CollectionStream.ofPartitions("testOrder.IntegerOrder", list))
        .addOutputStream(CollectionStream.empty("testOrder.OutputOrder"))
        .run();

    TaskAssert.that("testOrder.OutputOrder").comparePartitionsInOrder(expected);
  }
}
