package eu.leads.processor.infinispan;

import eu.leads.processor.common.infinispan.AcceptAllFilter;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.common.utils.ProfileEvent;
import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.context.Flag;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Apostolos Nydriotis on 2015/06/24.
 */
public class LeadsLocalReducerCallable<kOut, vOut> extends LeadsBaseCallable<kOut, Object>
    implements
    Serializable {

  private static final long serialVersionUID = 8028728191155715526L;
  private LeadsReducer<kOut, vOut> reducer = null;
  private LeadsCollector collector;
  private String prefix;
  private String site;

  public LeadsLocalReducerCallable(String cacheName, LeadsReducer<kOut, vOut> reducer,
                                   String prefix, String site) {
    super("{}", cacheName);
    this.reducer = reducer;
    collector = new LeadsCollector(1000, cacheName);
    collector.setOnMap(true);
    this.prefix = prefix;
    this.site = site;
  }

  public void setLocalSite(String localSite){
    collector.setLocalSite(localSite);
  }

  @Override
  public void executeOn(kOut key, Object value) {
//    LeadsIntermediateIterator<vOut> values = new LeadsIntermediateIterator<>((String) key, prefix,
//                                                                             imanager);
//    Iterator<vOut> values = ((List)value).iterator();
    Iterator<vOut> values = (Iterator<vOut>) value;
    reducer.reduce(key, values, collector);
  }


  @Override public String call() throws Exception {
    profCallable.end("call");
    if(!isInitialized){
      initialize();
    }
    profCallable.start("Call getComponent ()");
    final ClusteringDependentLogic cdl = inputCache.getAdvancedCache().getComponentRegistry().getComponent
        (ClusteringDependentLogic.class);
    profCallable.end();
    profCallable.start("Iterate Over Local Data");
    ProfileEvent profExecute = new ProfileEvent("GetIteratble " + this.getClass().toString(),profilerLog);
    int count=0;
    //    for(Object key : inputCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet()) {
    //      if (!cdl.localNodeIsPrimaryOwner(key))
    //        continue;
    Cache dataCache = inputCache.getCacheManager().getCache(prefix+".data");
    //Build data cache
    IntermediateKeyIndex index = null;
    for(Object listener : dataCache.getListeners()){
      if(listener instanceof LocalIndexListener){
        LocalIndexListener localIndexListener = (LocalIndexListener) listener;
        index = localIndexListener.getIndex();
        break;
      }
    }
    if(index == null){
      System.err.println("Index was not installed serious error exit...");
      System.exit(-1);
    }
    for(Map.Entry<String,Integer> entry : index.getKeysIterator()){
      LocalIndexKeyIterator iterator =
          (LocalIndexKeyIterator) index.getKeyIterator(entry.getKey(),entry.getValue());
      executeOn((kOut)entry.getKey(),iterator);
    }
    //            CloseableIterable iterable = inputCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).filterEntries(new LocalDataFilter<K,V>(cdl));
    //            profExecute.end();
    //            try {
    //                for (Object object : iterable) {
    //                    Map.Entry<K, V> entry = (Map.Entry<K, V>) object;
    //
    //                    //      V value = inputCache.get(key);
    //                    K key = (K) entry.getKey();
    //                    V value = (V) entry.getValue();
    //
    //                    if (value != null) {
    //                        profExecute.start("ExOn" + (++count));
    //                        executeOn((K) key, value);
    //                        profExecute.end();
    //                    }
    //                }
    //            }
    //            catch(Exception e){
    //                iterable.close();
    //                profilerLog.error("Exception in LEADSBASEBACALLABE " + e.getClass().toString());
    //                PrintUtilities.logStackTrace(profilerLog, e.getStackTrace());
    //            }
    profCallable.end();
    finalizeCallable();
    return embeddedCacheManager.getAddress().toString();
  }

  private Map<String, List<vOut>> createInMemoryDataStruct(Cache dataCache) {
    Map<String,List<vOut>>  result = new HashMap<>();
    CloseableIterable iterable = dataCache.getAdvancedCache().withFlags(
        Flag.CACHE_MODE_LOCAL).filterEntries(new AcceptAllFilter());
    try {
      for (Object object : iterable) {
        Map.Entry<ComplexIntermediateKey, vOut> entry = (Map.Entry<ComplexIntermediateKey, vOut>) object;
        List<vOut> list = result.get(entry.getKey().getKey());
        if (list == null) {
          list = new LinkedList<>();
          result.put(entry.getKey().getKey(), list);
        }
        list.add(entry.getValue());
        //
        //                    //      V value = inputCache.get(key);
        //                    K key = (K) entry.getKey();
        //                    V value = (V) entry.getValue();
        //
        //                    if (value != null) {
        //                        profExecute.start("ExOn" + (++count));
        //                        executeOn((K) key, value);
        //                        profExecute.end();
        //                    }
        //                }
      }
    }
    catch(Exception e){
      iterable.close();
      profilerLog.error("Exception in LEADSBASEBACALLABE " + e.getClass().toString());
      PrintUtilities.logStackTrace(profilerLog, e.getStackTrace());
    }
    return result;
  }
  @Override
  public void initialize() {
    super.initialize();
    collector.setOnMap(true);
    collector.setEmanager(emanager);
    collector.setSite(site);
    collector.setManager(embeddedCacheManager);
    collector.initializeCache(inputCache.getName(), imanager);
    collector.setCombiner(null);
    collector.setIsReduceLocal(true);
    collector.setUseCombiner(false);
    this.reducer.initialize();
  }

  @Override public void finalizeCallable() {
    System.err.println("finalize collector in reduce callable");
    collector.finalizeCollector();
    System.err.println("finalize reducelocalabe task");
    reducer.finalizeTask();
    System.err.println("finalize base");
    super.finalizeCallable();
  }
}
