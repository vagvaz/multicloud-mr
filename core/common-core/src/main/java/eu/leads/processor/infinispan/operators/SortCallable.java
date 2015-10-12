package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.ClusterInfinispanManager;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.core.TupleComparator;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

/**
 * Created by vagvaz on 9/24/14.
 */
public class SortCallable<K, V> implements

    DistributedCallable<K, V, String>, Serializable {
  transient private Cache<K, V> cache;
  private String[] sortColumns;
  private String[] types;
  private Boolean[] asceding;
  private Integer[] sign;
  transient private Set<K> keys;
  transient private Cache out;
  private String output;
  transient String address;
  private String prefix;

  public SortCallable(String[] sortColumns, Boolean[] ascending, String[] types, String output, String prefix) {
    this.sortColumns = sortColumns;
    this.asceding = ascending;
    this.types = types;
    this.output = output;
    this.prefix = prefix;
  }

  @Override public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
    this.cache = cache;
    keys = inputKeys;
    address = this.cache.getCacheManager().getAddress().toString();
    ClusterInfinispanManager manager = new ClusterInfinispanManager(cache.getCacheManager());
    out = (Cache) manager.getPersisentCache(prefix + "." + address);
  }

  @Override public String call() throws Exception {
    ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    final ClusteringDependentLogic cdl =
        cache.getAdvancedCache().getComponentRegistry().getComponent(ClusteringDependentLogic.class);
    for (Object key : cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet()) {
      if (!cdl.localNodeIsPrimaryOwner(key))
        continue;

      //        String valueString = (String)cache.get(key);
      //        if(valueString.equals(""))
      //          continue;
      //         tuples.add(new Tuple(valueString));
      Tuple tuple = (Tuple) cache.get(key);
      if (tuple != null)
        tuples.add(tuple);
    }
    Comparator<Tuple> comparator = new TupleComparator(sortColumns, asceding, types);
    Collections.sort(tuples, comparator);
    int counter = 0;
    for (Tuple t : tuples) {
      out.put(out.getName() + counter, t);
      //         out.put(out.getName()  + counter, t.asString());
      counter++;
    }
    tuples.clear();
    return out.getName();
  }
}
