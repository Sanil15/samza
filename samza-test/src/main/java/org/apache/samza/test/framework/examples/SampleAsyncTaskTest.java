package org.apache.samza.test.framework.examples;

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
import org.apache.samza.system.framework.utils.TaskAssert;
import org.apache.samza.test.framework.Mode;
import org.apache.samza.test.framework.TestTask;
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
        // Mimic a random callback delay ans send message
        RestCall call = new RestCall(envelope, collector, callback);
        call.start();
    }

    @Override
    public void close() throws Exception {
      // Close your client
    }
  }

  @Test
  public void testAsyncTaskWithConcurrency() throws Exception{
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
        .create(new AsyncRestTask(), new HashMap<>(), Mode.SINGLE_CONTAINER)
        .setTaskCallBackTimeoutMS(200)
        .setTaskMaxConcurrency(4)
        .addInputStream(CollectionStream.of("test.PageView", pageviews))
        .addOutputStream(CollectionStream.empty("test.Output"))
        .run();

    TaskAssert.that("test.Output").containsInAnyOrder(pageviews);

  }
}

class RestCall extends Thread{
  static Random random = new Random();
  IncomingMessageEnvelope _envelope;
  MessageCollector _messageCollector;
  TaskCallback _callback;
  RestCall(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCallback callback){
    this._envelope = envelope;
    this._callback = callback;
    this._messageCollector = collector;
  }
  @Override
  public void run(){
    System.out.println("Running " +  this.getName());
    try {
        // Let the thread sleep for a while.
        Thread.sleep(random.nextInt(150));
    } catch (InterruptedException e) {
      System.out.println("Thread " +  this.getName() + " interrupted.");
    }
    System.out.println("Thread " +  this.getName() + " exiting.");
    InMemorySystemUtils.PageView obj = (InMemorySystemUtils.PageView) _envelope.getMessage();
    _messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("test", "Output"), obj));
    _callback.complete();
  }

}
