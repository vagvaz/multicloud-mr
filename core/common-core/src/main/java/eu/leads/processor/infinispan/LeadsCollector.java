package eu.leads.processor.infinispan;


import eu.leads.processor.common.infinispan.ClusterInfinispanManager;
import eu.leads.processor.common.infinispan.EnsembleCacheUtils;
import eu.leads.processor.common.infinispan.InfinispanManager;

import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.Site;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class LeadsCollector<KOut, VOut> implements Collector<KOut, VOut>, Serializable {

  private static final long serialVersionUID = -602082107893975415L;
  private final AtomicInteger emitCount;
  private final int maxCollectorSize;
  protected transient BasicCache keysCache;
  protected transient BasicCache intermediateDataCache;
  protected transient BasicCache indexSiteCache;
  protected transient Cache counterCache;
  private transient BasicCache storeCache;
  private transient InfinispanManager imanager;
  private transient EmbeddedCacheManager manager;
  private transient EnsembleCacheManager emanager;
  private transient Logger log = null;
  private boolean onMap = true;  // TODO(ap0n): What is this?
  private int indexSite=-1;
  private String localSite;
  private String site;
  private String node;
  private String cacheName;
  private ComplexIntermediateKey baseIntermKey;
  private IndexedComplexIntermediateKey baseIndexedKey;
  private long localData;
  private long remoteData;

  public LeadsCollector(int maxCollectorSize, String collectorCacheName) {
    super();

    emitCount = new AtomicInteger();
    this.maxCollectorSize = maxCollectorSize;
    cacheName = collectorCacheName;
  }

  public LeadsCollector(int maxCollectorSize, String cacheName, InfinispanManager manager) {
    this.maxCollectorSize = maxCollectorSize;
    emitCount = new AtomicInteger();
    this.imanager = manager;
    this.cacheName = cacheName;
    storeCache = (BasicCache) emanager.getCache(cacheName, new ArrayList<>(emanager.sites()),
                                                EnsembleCacheManager.Consistency.DIST);
//    storeCache = (BasicCache) this.imanager.getPersisentCache(cacheName);
  }

  public Cache getCounterCache() {
    return counterCache;
  }

  public void setCounterCache(Cache counterCache) {
    this.counterCache = counterCache;
  }

  public BasicCache getIndexSiteCache() {
    return indexSiteCache;
  }

  public void setIndexSiteCache(BasicCache indexSiteCache) {
    this.indexSiteCache = indexSiteCache;
  }

  public BasicCache getIntermediateDataCache() {
    return intermediateDataCache;
  }

  public void setIntermediateDataCache(BasicCache intermediateDataCache) {
    this.intermediateDataCache = intermediateDataCache;
  }


  public String getLocalSite() {
    return localSite;
  }

  public void setLocalSite(String localSite) {
    this.localSite = localSite;
  }


  public BasicCache getKeysCache() {
    return keysCache;
  }

  public void setKeysCache(EnsembleCache keysCache) {
    this.keysCache = keysCache;
  }

  public BasicCache getCache() {
    return storeCache;
  }

  public EnsembleCacheManager getEmanager() {
    return emanager;
  }

  public void setEmanager(EnsembleCacheManager emanager) {
    this.emanager = emanager;
    if(localSite != null && !localSite.equals("")){
      for(Site s : emanager.sites()){
        indexSite++;
        if(s.getName().equals(localSite)){
          break;
        }
      }
    }

    System.out.println("Index Site is " + localSite + indexSite);
  }

  public EmbeddedCacheManager getManager() {
    return manager;
  }

  public void setManager(EmbeddedCacheManager manager) {
    this.manager = manager;

  }

  public String getSite() {
    return site;
  }

  public void setSite(String site) {
    this.site = site;
  }

  public String getNode() {
    return node;
  }

  public void setNode(String node) {
    this.node = node;
  }
  //  private LeadsCombiner combiner;

  public String getCacheName() {
    return cacheName;
  }

  public void setCacheName(String cacheName) {
    this.cacheName = cacheName;
  }

  public boolean isOnMap() {
    return onMap;
  }

  public void setOnMap(boolean onMap) {
    this.onMap = onMap;
  }

  public InfinispanManager getImanager() {
    return imanager;
  }

  public void setImanager(InfinispanManager imanager) {
    this.imanager = imanager;
  }

  public void initializeCache(String inputCacheName, InfinispanManager imanager) {
    this.imanager = imanager;
    log = LoggerFactory.getLogger(LeadsCollector.class);
//    storeCache = (Cache) imanager.getPersisentCache(cacheName);
    storeCache = emanager.getCache(cacheName, new ArrayList<>(emanager.sites()),
                                   EnsembleCacheManager.Consistency.DIST);
    node =imanager.getMemberName().toString();

    if (onMap) {
      intermediateDataCache = (BasicCache) emanager.getCache(storeCache.getName() + ".data",
                                                             new ArrayList<>(emanager.sites()),
                                                             EnsembleCacheManager.Consistency.DIST);
      //create Intermediate  keys cache name for data on the same Sites as outputCache;
      keysCache = (BasicCache) emanager.getCache(storeCache.getName() + ".keys",
                                                 new ArrayList<>(emanager.sites()),
                                                 EnsembleCacheManager.Consistency.DIST);
      //createIndexCache for getting all the nodes that contain values with the same key! in a mc
      indexSiteCache = (BasicCache) emanager.getCache(storeCache.getName() + ".indexed",
                                                      new ArrayList<>(emanager.sites()),
                                                      EnsembleCacheManager.Consistency.DIST);
      counterCache = manager.getCache(storeCache.getName()
                                      + "." + inputCacheName
                                      + "." + manager.getAddress().toString()
                                      + ".counters");
      baseIndexedKey = new IndexedComplexIntermediateKey(site, manager.getAddress().toString(),
                                                         inputCacheName);
      baseIntermKey = new ComplexIntermediateKey(site, manager.getAddress().toString(),
                                                 inputCacheName);
    }
  }

  public void emit(KOut key, VOut value) {

    if (onMap) {
      //      List<VOut> list = (List<VOut>) storeCache.get(key);

      //      if (list == null) {
      //        list = new LinkedList<>();
      //        //storeCache.put(key, list);
      //
      //      }
      //      list.add(value);
      //      emitCount.incrementAndGet();
      if(key.hashCode() % emanager.sites().size() == indexSite){
        localData += key.toString().length() +
                     site.length() +
                     node.length() + 4;  //cost for complex Intermediate key
        localData += value.toString().length();
      }
      else{
        remoteData += key.toString().length() +
            site.length() +
            node.length() + 4;  //cost for complex Intermediate key
        remoteData += value.toString().length();
      }
      Integer currentCount = (Integer) counterCache.get(key);
      if (currentCount == null) {
        currentCount = new Integer(0);
        baseIndexedKey.setKey(key.toString());
        EnsembleCacheUtils.putIfAbsentToCache(keysCache, key, key);
        EnsembleCacheUtils.putToCache(indexSiteCache, baseIndexedKey,
                                      new IndexedComplexIntermediateKey(baseIndexedKey));
//        Object o = indexSiteCache.get(baseIndexedKey.getUniqueKey());
//        if(o == null)
//        {
//          log.error("Could not add to indexedCache indexedKey " + baseIndexedKey.toString());
//        }
//        else{
//          log.error("successfully added to indexed cache " + baseIndexedKey.getUniqueKey() + "\n" + o.toString());
//        }
      } else {
        currentCount = currentCount + 1;
      }
      counterCache.put(key.toString(), currentCount);
      baseIntermKey.setKey(key.toString());
      baseIntermKey.setCounter(currentCount);
      ComplexIntermediateKey
          newKey =
          new ComplexIntermediateKey(baseIntermKey.getSite(), baseIntermKey.getNode(),
                                     key.toString(), baseIntermKey.getCache(), currentCount);
      EnsembleCacheUtils.putToCache(intermediateDataCache, newKey, value);
//      Object o = intermediateDataCache.get(newKey);
//      if(o == null){
//        System.err.println("\n\n\n\n\n#@#@INTERMEDIATE KEY " + newKey.toString() + " was not saved exiting" );
////        System.exit(-1);
//      }
//      else{
//        log.error("intermediate key " + newKey.toString() + " saved ");
//      }
    } else {
      if(key.hashCode() % emanager.sites().size() == indexSite){
        localData += key.toString().length() + value.toString().length();
      }
      else{
        remoteData += key.toString().length() + value.toString().length();
      }
      EnsembleCacheUtils.putToCache(storeCache, key, value);
//      emitCount.incrementAndGet();
    }
    // if (isOverflown() && mcc.hasCombiner()) {
    // combine(mcc, this);
    // }
//    Set<Object> keys = new HashSet<>();
//    keys.add(key);
  }

  public void spillMetricData(){
    EnsembleCache cache = emanager.getCache("metrics");
    Long oldValue = (Long) cache.get(localSite+":"+indexSite+"-"+node+"-"+site+"-"+storeCache.getName()+".local");
    if(oldValue == null){
      oldValue = new Long(localData);
    }
    else{
      oldValue += localData;
    }
    cache.put(localSite+":"+indexSite+"-"+node+"-"+site+"-"+storeCache.getName()+".local",oldValue);

    oldValue = (Long) cache.get(localSite+":"+indexSite+"-"+node+"-"+site+"-"+storeCache.getName()+".remote");
    if(oldValue == null){
      oldValue = new Long(remoteData);
    }
    else{
      oldValue += remoteData;
    }
    cache.put(localSite+":"+indexSite+"-"+node+"-"+site+"-"+storeCache.getName()+".remote",oldValue);
  }

  public void initializeCache(EmbeddedCacheManager manager) {
    imanager = new ClusterInfinispanManager(manager);
    storeCache = (Cache) imanager.getPersisentCache(cacheName);
  }


  public void reset() {
    storeCache.clear();
    emitCount.set(0);
  }

  //  public void emit(Map<KOut, List<VOut>> combined) {
  //    for (Entry<KOut, List<VOut>> e : combined.entrySet()) {
  //      KOut k = e.getKey();
  //      List<VOut> values = e.getValue();
  //      for (VOut v : values) {
  //        emit(k, v);
  //      }
  //    }
  //  }

  public boolean isOverflown() {
    return emitCount.get() > maxCollectorSize;
  }

  //  public void setCombiner(eu.leads.processor.infinispan.LeadsCombiner combiner) {
  //    this.combiner = combiner;
  //  }
  //
  //  public LeadsCombiner getCombiner() {
  //    return combiner;
  //  }
}
