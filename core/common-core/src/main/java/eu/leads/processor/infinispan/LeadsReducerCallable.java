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

public class LeadsReducerCallable<kOut, vOut> extends LeadsBaseCallable<kOut, Object> implements
                                                                                      Serializable {
  /**
   * tr
   */
  private static final long serialVersionUID = 3724554288677416505L;
  private LeadsReducer<kOut, vOut> reducer = null;
  private LeadsCollector collector;
  private String prefix;
  String site;

  public LeadsReducerCallable(String cacheName, LeadsReducer<kOut, vOut> reducer, String prefix) {
    super("{}", cacheName);
    this.reducer = reducer;
    collector = new LeadsCollector(1000, cacheName);
    collector.setOnMap(false);
    this.prefix = prefix;
  }

  public void setLocalSite(String localSite){
    collector.setLocalSite(localSite);
  }

  @Override public void setEnvironment(Cache<kOut, Object> cache, Set<kOut> inputKeys) {
    super.setEnvironment(cache, inputKeys);
  }

  @Override public void executeOn(kOut key, Object value) {
    //        LeadsIntermediateIterator<vOut> values = new LeadsIntermediateIterator<>((String) key,prefix,imanager);
    Iterator<vOut> values = (Iterator<vOut>) value;//((List)value).iterator();
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
//    Map<String,List<vOut>> map = createInMemoryDataStruct(dataCache);
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
//    for(Map.Entry<String,List<vOut>> entry : map.entrySet()) {
//      executeOn((kOut) entry.getKey(),entry.getValue());
//    }
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

    collector.setOnMap(false);
    collector.setEmanager(emanager);
    collector.initializeCache(inputCache.getName(), imanager);

    this.reducer.initialize();
  }
  //    public vOut call() throws Exception {
//        if (federationReducer == null) {
//            System.out.println(" Reducer not initialized ");
//        } else {
//           federationReducer.setCacheManager(inputCache.getCacheManager());
//            // System.out.println(" Run Reduce ");
//            vOut result = null;
////            System.out.println("inputCache Cache Size:"
////                    + this.inputCache.size());
////            for (Entry<kOut, List<vOut>> entry : inputCache.entrySet()) {
//          final ClusteringDependentLogic cdl = inputCache.getAdvancedCache().getComponentRegistry().getComponent(ClusteringDependentLogic.class);
//          for(Object ikey : inputCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet()){
//            if(!cdl.localNodeIsPrimaryOwner(ikey))
//              continue;
//                kOut key = (kOut)ikey;
//                List<vOut> list = inputCache.get(key);
//                if(list == null || list.size() == 0){
//                  continue;
//                }
//
//                vOut res = federationReducer.reduce(key, list.iterator());
//                if(res == null || res.toString().equals("")){
//                  ;
//                }
//                else
//                {
////                  outCache.put(key, res);
//                }
//            }
//
//            return result;
//        }
//        return null;
//    }

  @Override
  public void finalizeCallable() {
    System.err.println("reduce finalize reducer");
    reducer.finalizeTask();
    System.err.println("reducer finalizee collector");
    collector.finalizeCollector();
    System.err.println("finalzie super");
    super.finalizeCallable();
  }
}
