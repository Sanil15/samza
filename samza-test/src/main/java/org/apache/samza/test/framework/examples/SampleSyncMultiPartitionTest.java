package org.apache.samza.test.framework.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.framework.utils.StreamAssert;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.Mode;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.junit.Test;
import scala.Int;


public class SampleSyncMultiPartitionTest {

  @Test
  public void testSampleSyncMultiPartitionTestWithConcurrency() throws Exception {
    // Create a sample data
    Map<Integer, ArrayList<Integer>> input = new HashMap<Integer, ArrayList<Integer>>();
    Map<Integer, ArrayList<Integer>> expected = new HashMap<Integer, ArrayList<Integer>>();

    for (int i = 0; i < 2; i++) {
      input.put(i,new ArrayList<Integer>());
      expected.put(i, new ArrayList<Integer>());
      for (int j = 0; j < 4; j++) {
        input.get(i).add(j);
        expected.get(i).add(j * 10);
      }
    }
    System.out.println(Arrays.asList(input));
    System.out.println(Arrays.asList(expected));

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
    TestRunner
        .of(task)
        .addInputStream(CollectionStream.of("test.Integer", input))
        .addOutputStream(CollectionStream.empty("test.Output"))
        .run();

    StreamAssert.that("test.Output").contains(expected);
  }

}
