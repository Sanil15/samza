package org.apache.samza.test.framework;

import com.google.common.base.Preconditions;
import com.sun.org.apache.xpath.internal.operations.Mod;
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
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.StreamTask;
import org.apache.samza.test.framework.stream.CollectionStream;

public class TestTask {
  private StreamTask syncTask;
  private AsyncStreamTask asyncTask;
  public HashMap<String, String> configs;
  private InMemorySystemFactory factoryTest;
  private Mode mode;
  public static String systemName;
  public static final String JOB_NAME = "test-task";

  private TestTask(String systemName, StreamTask task, HashMap<String, String> config, Mode mode) {
    Preconditions.checkNotNull(systemName);
    Preconditions.checkNotNull(task);
    Preconditions.checkNotNull(config);
    this.systemName = systemName;
    this.syncTask = task;
    this.configs = config;
    this.mode = mode;
    factoryTest = new InMemorySystemFactory();

    // JOB Specific Config
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), systemName);
    configs.put(JobConfig.JOB_NAME(), JOB_NAME);

    if(mode.equals(mode.SINGLE_CONTAINER)) {
      configs.put(JobConfig.PROCESSOR_ID(), "1");
      configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
      configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
      configs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());
    }

    // InMemory System
    configs.put("systems." + systemName + ".samza.factory", InMemorySystemFactory.class.getName()); // system factory
  }

  private TestTask(String systemName, AsyncStreamTask task, HashMap<String, String> config, Mode mode) {
    Preconditions.checkNotNull(systemName);
    Preconditions.checkNotNull(task);
    Preconditions.checkNotNull(config);
    this.systemName = systemName;
    this.asyncTask = task;
    this.configs = config;
    factoryTest = new InMemorySystemFactory();
    this.mode = mode;
    // JOB Specific Config
    configs.put(JobConfig.JOB_DEFAULT_SYSTEM(), systemName);
    configs.put(JobConfig.JOB_NAME(), JOB_NAME);

    if(mode.equals(mode.SINGLE_CONTAINER)) {
      configs.put(JobConfig.PROCESSOR_ID(), "1");
      configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
      configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
      configs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());
    }

    // InMemory System
    configs.put("systems." + systemName + ".samza.factory", InMemorySystemFactory.class.getName()); // system factory
  }

  public static TestTask create(String systemName, StreamTask task, HashMap<String, String> config, Mode mode) {
    return new TestTask(systemName, task, config, mode);
  }

  public static TestTask create(String systemName, AsyncStreamTask task, HashMap<String, String> config, Mode mode) {
    return new TestTask(systemName, task, config, mode);
  }

  // Thread pool to run synchronous tasks in parallel.
  // Ordering is guarenteed
  public TestTask setJobContainerThreadPoolSize(Integer value) {
    Preconditions.checkNotNull(value);
    configs.put("job.container.thread.pool.size", String.valueOf(value));
    return this;
  }

  // Timeout for processAsync() callback. When the timeout happens, it will throw a TaskCallbackTimeoutException and shut down the container.
  public TestTask setTaskCallBackTimeoutMS(Integer value) {
    Preconditions.checkNotNull(value);
    configs.put("task.callback.timeout.ms", String.valueOf(value));
    return this;
  }

  // Max number of outstanding messages being processed per task at a time, applicable to both StreamTask and AsyncStreamTask.
  // Ordering is not guarenteed per partition
  public TestTask setTaskMaxConcurrency(Integer value) {
    Preconditions.checkNotNull(value);
    configs.put("task.max.concurrency", String.valueOf(value));
    return this;
  }

  public TestTask addInputStream(CollectionStream stream) {
    Preconditions.checkNotNull(stream);
    configs.putAll(stream.getStreamConfig());
    return this;
  }

  public TestTask addOutputStream(CollectionStream stream) {
    Preconditions.checkNotNull(stream);
    configs.putAll(stream.getStreamConfig());
    return this;
  }

  public void run() throws Exception {
    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    if (syncTask != null && asyncTask == null) {
      runner.runSyncTask(syncTask);
      runner.waitForFinish();
    } else if(asyncTask != null && syncTask == null) {
      runner.runAsyncTask(asyncTask);
      runner.waitForFinish();
    } else {
      throw new Exception("Test should use either one config async or sync, not both");
    }
  }

}

