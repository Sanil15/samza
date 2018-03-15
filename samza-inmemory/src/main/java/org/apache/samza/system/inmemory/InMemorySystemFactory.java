package org.apache.samza.system.inmemory;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;


/**
 *
 */
public class InMemorySystemFactory implements SystemFactory {
  private static final InMemoryManager MEMORY_MANAGER = new InMemoryManager();

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    return new InMemorySystemConsumer(MEMORY_MANAGER);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new InMemorySystemProducer(MEMORY_MANAGER);
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new InMemorySystemAdmin(MEMORY_MANAGER, config);
  }
}
