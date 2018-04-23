package org.apache.samza.test.framework;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.StreamTask;
import org.apache.samza.test.framework.stream.CollectionStream;


public class TestRunner {

  // Maintain the global job config
  private static Map<String, String> configs;

  private static Map<String, Object> systems;

  // Default job name
  private static final String JOB_NAME = "test-samza";

  // Either StreamTask or AsyncStreamTask exist
  private StreamTask syncTask;
  private AsyncStreamTask asyncTask;
  private StreamApplication app;

  // Mode defines single or multi container
  private Mode mode;


  private TestRunner(){
    this.configs = new HashMap<>();
    this.mode = Mode.SINGLE_CONTAINER;
    this.systems = new HashMap<String,Object>();

    // JOB Specific Config
    configs.put(JobConfig.JOB_NAME(), JOB_NAME);

    // Default Single Container configs
    configs.putIfAbsent(JobConfig.PROCESSOR_ID(), "1");
    configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
    configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    configs.putIfAbsent(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

  }

  private TestRunner(StreamTask task) {
    this();
    Preconditions.checkNotNull(task);
    this.syncTask = task;
  }

  private TestRunner(AsyncStreamTask task) {
    this();
    Preconditions.checkNotNull(task);
    this.asyncTask = task;
  }

  private TestRunner(StreamApplication app) {
    this();
    Preconditions.checkNotNull(app);
    this.app = app;
  }

  public static TestRunner of(StreamTask task) {
    return new TestRunner(task);
  }

  public static TestRunner of(AsyncStreamTask task) {
    return new TestRunner(task);
  }

  public static TestRunner of(StreamApplication app) {
    return new TestRunner(app);
  }

  public static InMemoryCollectionStreamSystem getOrIntializeInMemoryCollectionStreamSystem(String systemName) {
    Preconditions.checkState(systems != null);
    if(!systems.containsKey(systemName)) {
      InMemoryCollectionStreamSystem sys = InMemoryCollectionStreamSystem.create(systemName);
      systems.put(systemName, sys);
      configs.putAll(sys.getSystemConfigs());
    }
    return (InMemoryCollectionStreamSystem)systems.get(systemName);
  }

  public TestRunner addOverrideConfigs(Map<String,String> config) {
    Preconditions.checkNotNull(config);
    this.configs.putAll(config);
    return this;
  }

  public TestRunner setContainerMode(Mode mode) {
    Preconditions.checkNotNull(mode);
    if(mode.equals(Mode.MULTI_CONTAINER)){ // zk based config
      // zk based config
    }
    return this;
  }

  public TestRunner addInputStream(CollectionStream stream) {
    Preconditions.checkNotNull(stream);
    InMemoryCollectionStreamSystem system = getOrIntializeInMemoryCollectionStreamSystem(stream.getSystemName());
    system.addInput(stream.getSystemName(), stream.getStreamId(),stream.getInitPartitions());
    if(configs.containsKey(TaskConfig.INPUT_STREAMS()))
      configs.put(TaskConfig.INPUT_STREAMS(), configs.get(TaskConfig.INPUT_STREAMS()).concat(","+stream.getSystemName()+"."+stream.getStreamId()));
    stream.getStreamConfig().forEach((key,val) -> {
      configs.putIfAbsent((String)key, (String) val);
    });
    return this;
  }

  public TestRunner addOutputStream(CollectionStream stream) {
    Preconditions.checkNotNull(stream);
    configs.putAll(stream.getStreamConfig());
    return this;
  }

  public void run() throws Exception {
    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    if (syncTask != null && asyncTask == null && app == null) {
      runner.runSyncTask(syncTask);
      runner.waitForFinish();
    } else if(asyncTask != null && syncTask == null && app == null) {
      runner.runAsyncTask(asyncTask);
      runner.waitForFinish();
    } else if(asyncTask == null && syncTask == null && app != null) {
      runner.run(app);
      runner.waitForFinish();
    } else {
      throw new Exception("Test should use either one config async, application or sync");
    }
  }

}
