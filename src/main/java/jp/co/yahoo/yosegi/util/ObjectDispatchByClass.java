package jp.co.yahoo.yosegi.util;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class ObjectDispatchByClass<V> {
  @FunctionalInterface
  public interface Func<V> {
    public V get(Object obj);
  }

  private final Map<Class,V> cache = new HashMap<>();
  private final LinkedHashMap<Class,V> map = new LinkedHashMap<>();
  private V defaultValue = null;

  public ObjectDispatchByClass set(final Class klass, final V value) {
    map.put(klass, value);
    return this;
  }

  public ObjectDispatchByClass setDefault(final V value) {
    defaultValue = value;
    return this;
  }

  private V addCache(Class klass, V res) {
    cache.put(klass, res);
    return res;
  }

  /**
   * get dispatched value by searching head to tail.
   * cacching result for map dispatching
   */
  public Func create() {
    return obj -> {
      Class klass = obj.getClass();
      V res = cache.get(klass);
      if (Objects.nonNull(res)) {
        return res;
      }
      for (Map.Entry<Class,V> entry : map.entrySet()) {
        if (entry.getKey().isInstance(obj)) {
          return addCache(klass, entry.getValue());
        }
      }
      return addCache(klass, defaultValue);
    };
  }
}

