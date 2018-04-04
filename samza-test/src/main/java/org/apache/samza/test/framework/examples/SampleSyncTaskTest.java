package org.apache.samza.test.framework.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.inmemory.InMemorySystemUtils.*;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.system.framework.utils.TaskAssert;
import org.apache.samza.test.framework.Mode;
import org.apache.samza.test.framework.TestTask;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.junit.Test;


public class SampleSyncTaskTest {
  private static final String[] PAGEKEYS = {"inbox", "home", "search", "pymk", "group", "job"};

  @Test
  public void testSyncTaskWithConcurrency() throws Exception{
    // Create a sample data
    Random random = new Random();
    int count = 10;
    List<PageView> pageviews = new ArrayList<>();
    List<Integer> list = new ArrayList<Integer>();
    PageView[] check = new PageView[count];
    for (int i = 0; i < count; i++) {
      String pagekey = PAGEKEYS[random.nextInt(PAGEKEYS.length - 1)];
      int memberId = random.nextInt(10);
      pageviews.add(new PageView(pagekey, memberId));
      list.add(i);
    }


    // Create a StreamTask
    StreamTask task = new StreamTask() {
      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        PageView obj = (PageView)envelope.getMessage();
        collector.send(new OutgoingMessageEnvelope(new SystemStream("test","Output"), obj));
      }
    };

    // Run the test framework
    TestTask
        .create(task, new HashMap<>(), Mode.SINGLE_CONTAINER)
        .setJobContainerThreadPoolSize(4)
        .setTaskMaxConcurrency(4)
        .addInputStream(CollectionStream.of("test.SomeView", pageviews))
        .addOutputStream(CollectionStream.empty("test.Output"))
        .run();

    TaskAssert.that("test.Output").containsInAnyOrder(pageviews);
  }
}
