package org.apache.samza.test.framework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.StreamSpec;
import static org.apache.samza.test.framework.TestData.PageView;
import static org.junit.Assert.*;


public class RunTest {

  private static final String[] PAGEKEYS = {"inbox", "home", "search", "pymk", "group", "job"};

  public static void main(String args[]) throws IOException{

    /*
    * Data Injection Part
    * */
    Random random = new Random();
    int count = 10;
    TestData.PageView[] pageviews = new TestData.PageView[count];
    for (int i = 0; i < count; i++) {
      String pagekey = PAGEKEYS[random.nextInt(PAGEKEYS.length - 1)];
      int memberId = random.nextInt(10);
      pageviews[i] = new TestData.PageView(pagekey, memberId);
    }

    /*
    *  Low Level Config Impl
    * */
    int partitionCount = 4;
    Map<String, String> configs = new HashMap<>();

    // JOB Specific Config
    configs.put("job.default.system", "test");
    configs.put(JobConfig.JOB_NAME(), "test-eos-job");
    configs.put(JobConfig.PROCESSOR_ID(), "1");
    configs.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());

    // Task/Application
    configs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

    // InMemory System
    configs.put("systems.test.samza.factory", InMemorySystemFactoryTest.class.getName()); // system factory

    // Stream Specific
    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageviews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));
    configs.put("streams.PageView.dataset", Base64Serializer.serialize(pageviews)); // In Memory System specific config
    configs.put("serializers.registry.int.class", "org.apache.samza.serializers.IntegerSerdeFactory");
    configs.put("serializers.registry.json.class", TestData.PageViewJsonSerdeFactory.class.getName());

    /*
    *  High Level Stream Spec
    * */
//    StreamSpec spec = new StreamSpec("PageView","PageView","test",4);
//    InMemorySystemFactoryTest factoryTest = new InMemorySystemFactoryTest();
//    factoryTest.getAdmin("test", new MapConfig(configs)).createStream(spec);


    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    List<PageView> received = new ArrayList<>();
    final StreamApplication app = (streamGraph, cfg) -> {
      streamGraph.<KV<String, PageView>>getInputStream("PageView")
          .map(Values.create())
          .partitionBy(pv -> pv.getMemberId(), pv -> pv, "p1")
          .sink((m, collector, coordinator) -> {
            received.add(m.getValue());
          });
    };
    runner.run(app);
    runner.waitForFinish();

    assertEquals(received.size(), count * partitionCount);

  }

  public static final class Values {
    public static <K, V, M extends KV<K, V>> MapFunction<M, V> create() {
      return (M m) -> m.getValue();
    }
  }
}
