package org.apache.samza.test.framework.stream;


public class EventStream<T> {

  public static abstract class Builder<T> {
    public abstract Builder addElement();
    public abstract Builder addException();
    public abstract Builder advanceTimeTo(long time);
    public abstract EventStream<T> build();
  }

  public static <T> Builder<T> builder() {
    return null;
  }

}
