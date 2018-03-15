package org.apache.samza.system.inmemory;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;


public class InMemorySystemConfig extends MapConfig {
  // Serialized data set to initialize the consumer
  private final String SERIALIZED_DATA_SET = "streams.%s.dataset";
  private final String SERIALIZED_DATA_SET_DEFAULT = "";

  public InMemorySystemConfig(Config config) {
    super(config);
  }

  public String getSerializedDataSet(String streamId) {
    return getOrDefault(String.format(SERIALIZED_DATA_SET, streamId), SERIALIZED_DATA_SET_DEFAULT);
  }
}
