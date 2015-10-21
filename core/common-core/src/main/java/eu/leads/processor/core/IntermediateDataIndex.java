package eu.leads.processor.core;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by vagvaz on 10/21/15.
 */
public interface IntermediateDataIndex {
  Iterable<Map.Entry<String, Integer>> getKeysIterator();

  Iterator<Object> getKeyIterator(String key, Integer counter);

  //    80.156.73.113:11222;80.156.73.116:11222;80.156.73.123:11222;80.156.73.128:11222
  //    ;
  void flush();

  void put(Object key, Object value);

  void close();
}
