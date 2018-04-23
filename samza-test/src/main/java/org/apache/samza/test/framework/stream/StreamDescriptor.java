package org.apache.samza.test.framework.stream;

import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;


public class StreamDescriptor {
  String streamId;

  public String getStreamId() {
    return streamId;
  }

  public void setStreamId(String streamId) {
    this.streamId = streamId;
  }

  public static Input input(String streamId) {
    return new Input();
  }

  public static Output output(String streamId) {
    return new Output();
  }

  public static class Input<K,V>{

    public Input withKeySerde(Serde stringSerde) {
      return this;
    }
    public Input withMsgSerde(Serde stringSerde) {
      return this;
    }
  }
  public static class Output<K,V>{
    public Output withKeySerde(Serde stringSerde) {
      return this;
    }

    public Output withMsgSerde(Serde stringSerde) {
      return this;
    }
  }
}
