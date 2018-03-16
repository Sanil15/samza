package org.apache.samza.test.framework;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.inmemory.InMemorySystemFactory;


public class InMemorySystemFactoryTest implements SystemFactory {
  private static InMemorySystemFactory instance = null;

  private static InMemorySystemFactory getInstance() {
    if(instance == null)
      instance = new InMemorySystemFactory();
    return instance;
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    return getInstance().getConsumer(systemName, config, registry);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return getInstance().getProducer(systemName, config, registry);
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return getInstance().getAdmin(systemName,config);
  }
}
