package org.apache.samza.test.framework;

import com.sun.istack.internal.Nullable;
import java.util.Map;
import org.apache.samza.operators.KV;


public class CollectionStream<T> {
  public static <T> CollectionStream<T> of(Iterable<T> elems) { return null; }
  public static <T> CollectionStream<T> of(@Nullable T elem, @Nullable T... elems) { return null; }
  public static <T> CollectionStream<T> empty() {return null; }
  public static <K, V> CollectionStream<KV<K, V>> of(Map<K, V> elems) {return null;}
}
