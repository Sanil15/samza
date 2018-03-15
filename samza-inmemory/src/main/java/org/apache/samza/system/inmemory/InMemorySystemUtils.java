package org.apache.samza.system.inmemory;

import java.util.HashSet;
import java.util.Set;


public class InMemorySystemUtils {

  public static Set<Object> deserialize(String serializedDataSet) {
    // plugin base 64 decoder for now until deciding on the serialize format
    return new HashSet();
  }
}
