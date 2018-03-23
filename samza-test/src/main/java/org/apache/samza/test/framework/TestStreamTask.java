package org.apache.samza.test.framework;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.task.StreamTask;
import org.apache.samza.test.framework.factory.InMemorySystemFactoryTest;
import org.apache.samza.test.framework.stream.CollectionStream;

public class TestStreamTask {
  public StreamTask task;
  public HashMap<String,String> _config;
  private InMemorySystemFactoryTest factoryTest;
  public static final String SYSTEM_NAME = "test-samza";
  public static final String JOB_NAME = "test-samza-job";

  private TestStreamTask(StreamTask task, HashMap<String,String> config) {
    this.task = task;
    this._config = config;
    factoryTest = new InMemorySystemFactoryTest();

    // JOB Specific Config
    _config.put(JobConfig.JOB_DEFAULT_SYSTEM(), SYSTEM_NAME);
    _config.put(JobConfig.JOB_NAME(), JOB_NAME);
    _config.put(JobConfig.PROCESSOR_ID(), "1");
    _config.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
    _config.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());

    // Task/Application Config
    _config.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

    // InMemory System
    _config.put("systems."+SYSTEM_NAME+".samza.factory", InMemorySystemFactoryTest.class.getName()); // system factory

  }

  public static TestStreamTask create(StreamTask task, HashMap<String,String> config){
    return new TestStreamTask(task,config);
  }

  public TestStreamTask addInputStream(CollectionStream stream) {
    _config.putAll(stream.getStreamConfig());
    return this;
  }

  public TestStreamTask addOutputStream(CollectionStream stream) {
    _config.putAll(stream.getStreamConfig());
    return this;
  }

  public void run() {
    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(_config));
    runner.runTask(task);
    //runner.waitForFinish();
  };

}
