package org.apache.samza.test.framework.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.framework.utils.StreamAssert;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.ClosableTask;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.Mode;
import org.junit.Test;
import scala.Int;


public class SampleAsyncTaskTest {

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
    List<Integer> input = Arrays.asList(1,2,3,4,5);
    List<Integer> output = Arrays.asList(10,20,30,40,50);

//    // Run the test framework
//    TestRunner
//        .of(new AsyncRestTask())
//        .setTaskCallBackTimeoutMS(200)
//        .addInputStream(CollectionStream.of("test.input", input))
//        .addOutputStream(CollectionStream.empty("test.Output"))
//        .run();


    StreamAssert.that("test.Output").contains(output);

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
    Integer obj = (Integer) _envelope.getMessage();
    _messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("test", "Output"), obj * 10));
    _callback.complete();
  }

}
