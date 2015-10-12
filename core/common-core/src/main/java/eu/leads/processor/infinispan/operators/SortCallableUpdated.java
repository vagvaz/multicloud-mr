package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.EnsembleCacheUtilsSingle;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.core.TupleComparator;
import eu.leads.processor.infinispan.LeadsBaseCallable;
import eu.leads.processor.infinispan.LocalDataFilter;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.context.Flag;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.infinispan.ensemble.cache.distributed.DistributedEnsembleCache;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

import java.util.*;

/**
 * Created by vagvaz on 2/20/15.
 */
public class SortCallableUpdated<K, V> extends LeadsBaseCallable<K, V> {

  private String[] sortColumns;
  private String[] types;
  private Boolean[] asceding;
  private Integer[] sign;
  transient private List<Tuple> tuples;
  //  private String output;
  transient String address;
  private String prefix;
  private String addressesCacheName;
  private long limit = -1;
  private transient EnsembleCache addressesCache;

  public SortCallableUpdated() {
    super();
  }

  //  public SortCallableUpdated(String configString, String output){
  public SortCallableUpdated(String[] sortColumns, Boolean[] ascending, String[] types, String output, String prefix,
      long rowcount) {
    super("{}", output);
    this.sortColumns = sortColumns;
    this.asceding = ascending;
    this.types = types;
    this.output = output;
    this.prefix = prefix;
    this.addressesCacheName = output.substring(0, output.lastIndexOf(".")) + ".addresses";
    this.limit = rowcount;
  }

  @Override public void initialize() {
    super.initialize();
    address = inputCache.getCacheManager().getAddress().toString();
    //    outputCache = (Cache) imanager.getPersisentCache(prefix+"."+address);
    emanager.start();
    outputCache = emanager
        .getCache(prefix + "." + LQPConfiguration.getInstance().getMicroClusterName() + "." + imanager.getMemberName(),
            new ArrayList<>(emanager.sites()), EnsembleCacheManager.Consistency.DIST);
    addressesCache =
        emanager.getCache(addressesCacheName, new ArrayList<>(emanager.sites()), EnsembleCacheManager.Consistency.DIST);
    tuples = new ArrayList<>(100);
    collector.setOnMap(false);
    collector.setManager(this.embeddedCacheManager);
    collector.setEmanager(emanager);
    collector.setSite(LQPConfiguration.getInstance().getMicroClusterName());
    collector.initializeCache(inputCache.getName(), imanager);
  }

  @Override public String call() throws Exception {
    if (!isInitialized) {
      initialize();
    }
    final ClusteringDependentLogic cdl =
        inputCache.getAdvancedCache().getComponentRegistry().getComponent(ClusteringDependentLogic.class);
    Object filter = new LocalDataFilter<K, V>(cdl);
    CloseableIterable iterable = inputCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
        .filterEntries((KeyValueFilter<? super K, ? super V>) filter);
    //        .converter((Converter<? super K, ? super V, ?>) filter);

    try {

      for (Object object : iterable) {
        Map.Entry<K, V> entry = (Map.Entry<K, V>) object;

        //      V value = inputCache.get(key);
        K key = (K) entry.getKey();
        V value = (V) entry.getValue();
        if (value != null) {
          executeOn((K) key, value);
        }
      }
      iterable.close();
    } catch (Exception e) {
      iterable.close();
      System.err.println("Exception in LEADSBASEBACALLABE " + e.getClass().toString());
      //        PrintUtilities.logStackTrace(profilerLog, e.getStackTrace());
      e.printStackTrace();
    }
    this.endCallable();
    finalizeCallable();
    return outputCache.getName();
  }

  @Override public void executeOn(K key, V value) {


    //      String valueString = (String)value;
    //      if(!valueString.equals("")){
    //
    //      tuples.add(new Tuple(valueString));
    if (value != null) {
      tuples.add((Tuple) value);
    }

  }

  public void endCallable() {
    EnsembleCacheUtilsSingle ensembleCacheUtilsSingle = new EnsembleCacheUtilsSingle();
    ensembleCacheUtilsSingle.initialize(emanager);
    Comparator<Tuple> comparator = new TupleComparator(sortColumns, asceding, types);
    Collections.sort(tuples, comparator);
    long counter = 0;
    String prefix = outputCache.getName();
    emanager.start();
    if (limit > 0) {
      System.err.println("SORTCALLABLE: USING LIMIT " + limit);
    }

    for (Tuple t : tuples) {
      //      System.out.println("output to " + outputCache.getName() + "   key " + prefix + counter);
      if (limit > 0 && counter >= limit) {
        break;
      }
      ensembleCacheUtilsSingle.putToCache(outputCache, prefix + counter, t);

      //      outputCache.put(outputCache.getName()  + counter, t);
      counter++;
    }
    //    EnsembleCacheUtils.waitForAllPuts();
    //    addressesCache.put(prefix+"."+LQPConfiguration.getInstance().getMicroClusterName()
    //                         +"."+imanager.getMemberName(),
    //                        prefix+"."+LQPConfiguration.getInstance().getMicroClusterName()
    //                          +"."+imanager.getMemberName());

    EnsembleCacheManager em = new EnsembleCacheManager(ensembleHost);
    em.start();
    addressesCache =
        em.getCache(addressesCacheName, new ArrayList<>(emanager.sites()), EnsembleCacheManager.Consistency.DIST);
    ((DistributedEnsembleCache) addressesCache).start();
    //    EnsembleCacheUtils.putToCacheDirect(addressesCache,outputCache.getName(),outputCache.getName());
    try {
      addressesCache.put(outputCache.getName(), outputCache.getName());
    } catch (Exception e) {
      e.printStackTrace();
      try {
        addressesCache = emanager
            .getCache(addressesCacheName, new ArrayList<>(emanager.sites()), EnsembleCacheManager.Consistency.DIST);
        addressesCache.put(outputCache.getName(), outputCache.getName());
      } catch (Exception ee) {
        ee.printStackTrace();
      }
    }
    tuples.clear();
    try {
      ensembleCacheUtilsSingle.waitForAllPuts();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
