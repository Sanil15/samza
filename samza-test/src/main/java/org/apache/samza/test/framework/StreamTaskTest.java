package org.apache.samza.test.framework;

public class StreamTaskTest {

  public StreamTaskTest addInputStream(String systemStream, TestProtoType.CollectionStream stream) {
    return this;
  }

  public StreamTaskTest addOutputStream(String systemStream, StreamTaskTest stream) {
    return this;
  }

  public void run() { };
}
