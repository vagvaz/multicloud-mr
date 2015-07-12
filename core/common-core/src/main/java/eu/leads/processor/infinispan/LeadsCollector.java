package eu.leads.processor.infinispan;


import eu.leads.processor.common.infinispan.ClusterInfinispanManager;
import eu.leads.processor.common.infinispan.EnsembleCacheUtils;
import eu.leads.processor.common.infinispan.InfinispanManager;

import eu.leads.processor.conf.LQPConfiguration;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LeadsCollector<KOut, VOut> implements Collector<KOut, VOut>, Serializable {

  private static final long serialVersionUID = -602082107893975415L;
  private  Integer emitCount;
  private  int maxCollectorSize = 1000;
  private double percent = .75;
  private LeadsCombiner<KOut,VOut> combiner;
  protected transient BasicCache keysCache;
  protected transient BasicCache intermediateDataCache;
  protected transient BasicCache indexSiteCache;
  protected transient Cache counterCache;
  private transient BasicCache storeCache;
  private transient InfinispanManager imanager;
  private transient EmbeddedCacheManager manager;
  private transient EnsembleCacheManager emanager;
  private transient Logger log = null;
  private transient Map<KOut,List<VOut>> buffer;
  private boolean onMap = true;
  private boolean isReduceLocal = false;
  private boolean useCombiner = true;
  private int indexSite=-1;
  private String localSite;
  private String site;
  private String node;
  private String cacheName;
  private ComplexIntermediateKey baseIntermKey;
  private IndexedComplexIntermediateKey baseIndexedKey;
  private long localData;
  private long remoteData;
  protected Map<KOut, List<VOut>> combinedValues;

  public LeadsCollector(int maxCollectorSize, String collectorCacheName) {
    super();

//    this.maxCollectorSize = maxCollectorSize;
    cacheName = collectorCacheName;
    emitCount = 0;
  }

  public LeadsCollector(int maxCollectorSize, String cacheName, InfinispanManager manager) {
//    this.maxCollectorSize = maxCollectorSize;
    emitCount = 0;
    this.imanager = manager;
    this.cacheName = cacheName;
    storeCache = (BasicCache) emanager.getCache(cacheName, new ArrayList<>(emanager.sites()),
                                                EnsembleCacheManager.Consistency.DIST);
//    storeCache = (BasicCache) this.imanager.getPersisentCache(cacheName);
  }

  public LeadsCombiner<KOut, VOut> getCombiner() {
    return combiner;
  }

  public void setCombiner(LeadsCombiner<KOut, VOut> combiner) {
    this.combiner = combiner;
    maxCollectorSize = LQPConfiguration.getInstance().getConfiguration().getInt("node.combiner.buffersize",10000);
    percent = LQPConfiguration.getInstance().getConfiguration().getInt("node.combiner.percent",75);
    percent /= 100;
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

  public boolean isReduceLocal() {
    return isReduceLocal;
  }

  public void setIsReduceLocal(boolean isReduceLocal) {
    this.isReduceLocal = isReduceLocal;
  }

  public boolean isUseCombiner() {
    return useCombiner;
  }

  public void setUseCombiner(boolean useCombiner) {
    this.useCombiner = useCombiner;
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
    emitCount = 0;
    buffer = new HashMap<>();
//    storeCache = (Cache) imanager.getPersisentCache(cacheName);
    storeCache = emanager.getCache(cacheName, new ArrayList<>(emanager.sites()),
                                   EnsembleCacheManager.Consistency.DIST);
    node =imanager.getMemberName().toString();
    site = LQPConfiguration.getInstance().getMicroClusterName();
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
      if(isReduceLocal){
        output(key,value);
      }
      else{
        if(useCombiner){
          List<VOut> values = buffer.get(key);
          if(values == null) {
            values = new LinkedList<>();
          }
          values.add(value);
          buffer.put(key, values);
          emitCount++;
          if(isOverflown()){
            combine(false);
          }
        }
        else{
          output(key,value);
        }
      }

    } else {
      if(key.hashCode() % emanager.sites().size() == indexSite){
        localData += key.toString().length() + value.toString().length();
      }
      else{
        remoteData += key.toString().length() + value.toString().length();
      }
      EnsembleCacheUtils.putToCache(storeCache, key, value);
    }
  }

  private void combine(boolean force) { //force the output of values
    LeadsCollector localCollector = new LocalCollector(0,"");
    for(Map.Entry<KOut,List<VOut>> entry : buffer.entrySet()){
      combiner.reduce(entry.getKey(),entry.getValue().iterator(),localCollector);
    }
    Map<KOut,List<VOut>> combinedValues = localCollector.getCombinedValues();
    if( force  || (combinedValues.size() >= maxCollectorSize*percent )) {
      for (Map.Entry<KOut, List<VOut>> entry : combinedValues.entrySet()) {
        for (VOut v : entry.getValue()) {
          output(entry.getKey(), v);
        }
      }
      buffer.clear();
      emitCount = 0; // the size is 0 since we have written everything
    }
    else{
      buffer.clear();
      buffer = combinedValues;
      emitCount = buffer.size(); // the size is only one per each key
    }
  }

  private void output(KOut key, VOut value) {
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
//      EnsembleCacheUtils.putIfAbsentToCache(keysCache, key, key);
//      EnsembleCacheUtils.putToCache(indexSiteCache,  new IndexedComplexIntermediateKey(baseIndexedKey),
//          new IndexedComplexIntermediateKey(baseIndexedKey));
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
  }

  public void finalizeCollector(){
    combine(true);
    spillMetricData();
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
    cache.put(localSite + ":" + indexSite + "-" + node + "-" + site + "-" + storeCache.getName()
        + ".local", oldValue);

    oldValue = (Long) cache.get(localSite+":"+indexSite+"-"+node+"-"+site+"-"+storeCache.getName()+".remote");
    if(oldValue == null){
      oldValue = new Long(remoteData);
    }
    else{
      oldValue += remoteData;
    }
    cache.put(localSite + ":" + indexSite + "-" + node + "-" + site + "-" + storeCache.getName()
        + ".remote", oldValue);
  }

  public void initializeCache(EmbeddedCacheManager manager) {
    imanager = new ClusterInfinispanManager(manager);
    storeCache = (Cache) imanager.getPersisentCache(cacheName);
  }


  public void reset() {
    storeCache.clear();
    emitCount = 0;
  }



  public boolean isOverflown() {
    return emitCount.intValue() >=   maxCollectorSize;
  }

  public Map<KOut, List<VOut>> getCombinedValues() {
    return combinedValues;
  }

  public void setCombinedValues(Map<KOut, List<VOut>> combinedValues) {
    this.combinedValues = combinedValues;
  }
}
