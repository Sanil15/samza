package org.apache.samza.test.framework;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.task.StreamTask;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.apache.samza.test.framework.stream.EventStream;
import org.apache.samza.test.framework.stream.FileStream;


public class TestApplication<T> {
  private StreamApplication application;
  public HashMap<String, String> configs;
  private InMemorySystemFactory factoryTest;
  private Mode mode;
  public static final String SYSTEM_NAME = "test-samza-application";
  public static final String JOB_NAME = "test-application";

  private TestApplication( HashMap<String, String> configs, Mode mode) {
    Preconditions.checkNotNull(configs);
    Preconditions.checkNotNull(mode);

    this.configs = configs;
    this.mode = mode;
    factoryTest = new InMemorySystemFactory();

    // JOB Specific Config
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), SYSTEM_NAME);
    configs.put(JobConfig.JOB_NAME(), JOB_NAME);

    if(mode.equals(mode.SINGLE_CONTAINER)) {
      configs.put(JobConfig.PROCESSOR_ID(), "1");
      configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
      configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
      configs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());
    }

    // InMemory System
    configs.put("systems." + SYSTEM_NAME + ".samza.factory", InMemorySystemFactory.class.getName()); // system factory

    /**
     * Configration of Stream Graph, stream app
     */
  }

  public static TestApplication create(HashMap<String, String> configs, Mode mode) {
    return new TestApplication(configs, mode);
  }

  public <T> MessageStream<T> getInputStream(CollectionStream<T> stream) {return null;}

  public <T> MessageStream<T> getInputStream(EventStream<T> stream) {return null;}

  public <T> MessageStream<T> getInputStream(FileStream<T> stream) {return null;}

  public void run() {
    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    runner.run(application);
    runner.waitForFinish();
  };
}
