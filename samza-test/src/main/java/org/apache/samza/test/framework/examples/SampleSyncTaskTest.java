package org.apache.samza.test.framework.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.apache.samza.test.framework.stream.StreamDescriptor;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Test;


public class SampleSyncTaskTest {

  @Test
  public void testSyncTaskWithConcurrency() throws Exception{
    // Create a sample data
    List<Integer> input = Arrays.asList(1,2,3,4,5);
    List<Integer> output = Arrays.asList(10,20,30,40,50);

    // Create a StreamTask and pass it to factory
    StreamTask task = new StreamTask() {
      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Integer obj = (Integer)envelope.getMessage();
        collector.send(new OutgoingMessageEnvelope(new SystemStream("test","output"), obj * 10));
      }
    };


    CollectionStream<Integer> intStream = CollectionStream.of("test","input", input);
    CollectionStream<Integer> out =  CollectionStream.empty("test", "output");

    TestRunner
        .of(task)
        .addInputStream(intStream)
        .addOutputStream(out)
        .run();

    Assert.assertThat(out.getStreamState(),IsIterableContainingInOrder.contains(output.toArray()));



























//    StreamDescriptor.Input input = StreamDescriptor.<String, String>input("input")
//        .withKeySerde(new StringSerde("UTF-8"))
//        .withMsgSerde(new StringSerde("UTF-8"));
//
//    StreamDescriptor.Output output = StreamDescriptor.<String, String>output("output")
//        .withKeySerde(new StringSerde("UTF-8"))
//        .withMsgSerde(new StringSerde("UTF-8"));
//
//    InMemoryTestSystem testingSystem = InMemoryTestSystem.create("samza-test")
//        .addInput("input" /*replaced by passing StreamDescriptor*/, inputList)
//        .addOutput("output" /*replaced by passing StreamDescriptor*/);
//
//
//    // JOB Specific Config
//    configs.put(JobConfig.JOB_NAME(), JOB_NAME);
//
//    // Default Single Container configs
//    configs.putIfAbsent(JobConfig.PROCESSOR_ID(), "1");
//    configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
//    configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
//    configs.putIfAbsent(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());
//
//
//    StreamTaskApplication app = StreamApplications.createStreamTaskApp(config, new MyStreamTaskFactory());
//    app.addInputs(input).addOutputs(output).runAsTest();
//
//    Assert.assertThat(testingSystem.getStreamState("input"), IsIterableContainingInOrder.contains(inputList.toArray()));

  }
}
