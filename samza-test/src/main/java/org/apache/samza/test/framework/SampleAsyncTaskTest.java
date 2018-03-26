package org.apache.samza.test.framework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.inmemory.InMemorySystemUtils;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.ClosableTask;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.junit.Test;


public class SampleAsyncTaskTest {
  private static final String[] PAGEKEYS = {"inbox", "home", "search", "pymk", "group", "job"};

  public static class AsyncRestTask implements AsyncStreamTask, InitableTask, ClosableTask {
    @Override
    public void init(Config config, TaskContext taskContext) throws Exception {
      // Your initialization of web client code goes here
    }

    @Override
    public void processAsync(IncomingMessageEnvelope envelope, MessageCollector collector,
        TaskCoordinator coordinator, final TaskCallback callback) {

      try { // Mimic a callback delay
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      InMemorySystemUtils.PageView obj = (InMemorySystemUtils.PageView) envelope.getMessage();
      collector.send(new OutgoingMessageEnvelope(new SystemStream("test-samza", "Output"), obj.getPageKey()));
    }

    @Override
    public void close() throws Exception {
      // client.close();
    }
  }

  @Test
  public void testAsyncTask() throws Exception{
    Random random = new Random();
    int count = 10;
    List<InMemorySystemUtils.PageView> pageviews = new ArrayList<>();

    // Creating a sample data
    for (int i = 0; i < count; i++) {
      String pagekey = PAGEKEYS[random.nextInt(PAGEKEYS.length - 1)];
      int memberId = random.nextInt(10);
      pageviews.add(new InMemorySystemUtils.PageView(pagekey, memberId));
    }

    // Run the test framework
    TestTask
        .create("test-samza", new AsyncRestTask(), new HashMap<>())
        .setJobContainerThreadPoolSize(4)
        .addInputStream(CollectionStream.of("PageView", pageviews))
        .addOutputStream(CollectionStream.empty("Output"))
        .run();
    TaskAssert.that("test-samza", "Output").containsInAnyOrder(pageviews);
  }
}
