package eu.leads.processor.core;

import org.apache.commons.collections.keyvalue.AbstractMapEntry;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by vagvaz on 12/11/15.
 */
public class MapDBKeyIterator implements Iterable<Map.Entry<String, Integer>>,Iterator<Map.Entry<String, Integer>> {
  Iterator<Map.Entry<String, Integer>> mapdbIterator;
  public MapDBKeyIterator(Set<Map.Entry<String, Integer>> entries) {
    mapdbIterator = entries.iterator();
  }

  @Override public Iterator<Map.Entry<String, Integer>> iterator() {
    return this;
  }

  @Override public boolean hasNext() {
    return mapdbIterator.hasNext();
  }

  @Override public Map.Entry<String, Integer> next() {
    Map.Entry<String,Integer> entry = mapdbIterator.next();
    return new AbstractMap.SimpleEntry<String, Integer>(entry.getKey().substring(0,entry.getKey().lastIndexOf("{}")),entry.getValue());
  }

  @Override public void remove() {

  }
}
