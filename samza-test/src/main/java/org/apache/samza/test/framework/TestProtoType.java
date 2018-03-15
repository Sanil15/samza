package org.apache.samza.test.framework;

import com.google.common.collect.ImmutableMap;
import com.sun.istack.internal.Nullable;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.Arrays;
import java.util.Map;

public class TestProtoType {

  public static final class CollectionStream<T> {
    public static <T> CollectionStream<T> of(Iterable<T> elems) { return null; }
    public static <T> CollectionStream<T> of(@Nullable T elem, @Nullable T... elems) { return null; }
    public static <T> CollectionStream<T> empty() {return null; }
    public static <K, V> CollectionStream<KV<K, V>> of(Map<K, V> elems) {return null;}
  }
  /*
  * Offline data injection eg files from local disk
  */
  public static class FileStream<T> {
    public static <T> FileStream<T> of(String fileUri) {return null;}
  }

  public static class TestStream<T> {
    public static <T> Builder<T> builder() {return null;}

    public static abstract class Builder<T> {
      public abstract Builder addElement();
      public abstract Builder addException();
      public abstract Builder advanceTimeTo(long time);
      public abstract TestStream<T> build();
    }
  }

  /**
   * An example of low-level api job test
   */
  public static void testLowLevelApi() {
    StreamTask task = new StreamTask() {
      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
      }
    };

    TestApplication.TaskTest app = TestApplication.create(task, new MapConfig());
    app.addInputStream("queuing.PageViewEvent", CollectionStream.of(ImmutableMap.of(
        "1", "PageView1",
        "2", "PageView2"
    )));
    app.addOutputStream("queuing.MyPageViewEvent", CollectionStream.empty());

//    TaskAssert.that("queuing.MyPageViewEvent").contains(ImmutableMap.of(
//        "1", "PageView1",
//        "2", "PageView2"
//    ));

    app.run();
  }



  public static class TestApplication {
    /**
     * Create high-level api test application
     */
    public static AppTest create(Config config) {return null;}

    public static TaskTest create(StreamTask streamTask, Config config) {return null;}

    public static class AppTest {
      public <T> MessageStream<T> getInputStream(TestStream<T> stream) {return null;}
      public <T> MessageStream<T> getInputStream(CollectionStream<T> stream) {return null;}
      public <T> MessageStream<T> getInputStream(FileStream<T> stream) {return null;}
      public void run() { };
    }

    public static class TaskTest {
      public TaskTest addInputStream(String systemStream, TestStream stream) {return this;}
      public TaskTest addInputStream(String systemStream, CollectionStream stream) {return this;}
      public TaskTest addInputStream(String systemStream, FileStream stream) {return this;}

      public TaskTest addOutputStream(String systemStream, CollectionStream stream) {return this;}
      public void run() { };
    }
  }
  /**
   * An example of high-level api job test
   */
  public static void testHighLevelApi() {
    TestApplication.AppTest app = TestApplication.create(new MapConfig());
    CollectionStream<String> input1 = CollectionStream.of("1", "2", "4");
    MessageStream<String> stream = app
        .getInputStream(input1)
        .map(s -> "processed " + s);
    //StreamAssert.that(stream).containsInAnyOrder(Arrays.asList("processed 1", "processed 2", "processed 4"));

    app.run();
  }

}