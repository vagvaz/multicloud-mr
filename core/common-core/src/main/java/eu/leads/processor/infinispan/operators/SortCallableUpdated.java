package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.EnsembleCacheUtils;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.core.TupleComparator;
import eu.leads.processor.infinispan.LeadsBaseCallable;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.context.Flag;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by vagvaz on 2/20/15.
 */
public class SortCallableUpdated<K,V> extends LeadsBaseCallable<K,V> {

  private String[] sortColumns;
  private String[] types;
  private Boolean[] asceding;
  private Integer[] sign;
  transient private List<Tuple> tuples;
  private String output;
  transient String address;
  private String prefix;
  private String addressesCacheName;
  private transient BasicCache addressesCache;

  //  public SortCallableUpdated(String configString, String output){
  public SortCallableUpdated(String[] sortColumns, Boolean[] ascending, String[] types, String output,
                             String prefix){
    super("{}", output);
    this.sortColumns = sortColumns;
    this.asceding = ascending;
    this.types = types;
    this.output = output;
    this.prefix = prefix;
    this.addressesCacheName = output.substring(0,output.lastIndexOf("."))+".addresses";
  }

  @Override public void initialize() {
    super.initialize();
    address = inputCache.getCacheManager().getAddress().toString();
//    outputCache = (Cache) imanager.getPersisentCache(prefix+"."+address);
    outputCache = emanager.getCache(prefix+"."+LQPConfiguration.getInstance().getMicroClusterName()
                                                 +"."+imanager.getMemberName(),new ArrayList<>(emanager.sites()),
        EnsembleCacheManager.Consistency.DIST);
    addressesCache = emanager.getCache(addressesCacheName,new ArrayList<>(emanager.sites()),
        EnsembleCacheManager.Consistency.DIST);
    tuples = new ArrayList<>(100);
  }

  @Override public String call() throws Exception {
    if(!isInitialized){
      initialize();
    }
    final ClusteringDependentLogic cdl = inputCache.getAdvancedCache().getComponentRegistry().getComponent
                                                                                                      (ClusteringDependentLogic.class);
    for(Object key : inputCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet()) {
      if (!cdl.localNodeIsPrimaryOwner(key))
        continue;
      V value = inputCache.get(key);

      if (value != null) {
        executeOn((K)key, value);
      }
    }
    finalizeCallable();
    return outputCache.getName();
  }

  @Override public void executeOn(K key, V value) {


//      String valueString = (String)value;
//      if(!valueString.equals("")){
//
//      tuples.add(new Tuple(valueString));
    if(value != null ){
      tuples.add((Tuple) value);
    }

  }

  @Override public void finalizeCallable(){
    Comparator<Tuple> comparator = new TupleComparator(sortColumns,asceding,types);
    Collections.sort(tuples, comparator);
    int counter = 0;
    String prefix = outputCache.getName();
    for (Tuple t : tuples) {
      System.out.println("output to " + outputCache.getName() + "   key " + prefix + counter);
      EnsembleCacheUtils.putToCache(outputCache,prefix + counter, t);

//      outputCache.put(outputCache.getName()  + counter, t);
      counter++;
    }
//    addressesCache.put(prefix+"."+LQPConfiguration.getInstance().getMicroClusterName()
//                         +"."+imanager.getMemberName(),
//                        prefix+"."+LQPConfiguration.getInstance().getMicroClusterName()
//                          +"."+imanager.getMemberName());
    EnsembleCacheUtils.putToCache(addressesCache,outputCache.getName(),outputCache.getName());
    tuples.clear();
    super.finalizeCallable();
  }
}
