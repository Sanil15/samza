package org.apache.samza.test.framework.examples;

import java.util.Arrays;
import java.util.List;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.system.framework.utils.StreamAssert;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.junit.Test;


class MyStreamApplication implements StreamApplication {
  @Override
  public void init(StreamGraph graph, Config config) {
    MessageStream<Integer> pageViews = graph.getInputStream("input");

    pageViews.map(s -> s * 10)
        .sendTo(graph.getOutputStream("output"));
  }
}

public class SampleStreamApplicationTest {
  @Test
  public void testStreamApplication() throws Exception{
    // Create a sample data
    List<Integer> input = Arrays.asList(1,2,3,4,5);
    List<Integer> output = Arrays.asList(10,20,30,40,50);

    TestRunner
        .of(new MyStreamApplication())
        .addInputStream(CollectionStream.of("test.input",input))
        .addOutputStream(CollectionStream.empty("test.output"))
        .run();

    StreamAssert.that("test.output").containsInAnyOrder(output);

  }
}
