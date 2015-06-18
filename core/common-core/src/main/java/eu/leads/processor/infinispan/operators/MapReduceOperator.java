package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.infinispan.*;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.vertx.java.core.json.JsonObject;

import java.util.Set;
import java.util.UUID;

/**
 * Created by tr on 19/9/2014.
 */
public abstract class MapReduceOperator extends BasicOperator{
//  protected transient BasicCache inputCache;
  protected transient BasicCache intermediateCache;
  protected transient BasicCache  outputCache;
  protected transient BasicCache  keysCache;
  protected transient BasicCache intermediateDataCache;
  protected transient BasicCache indexSiteCache;
  protected String inputCacheName;
  protected String outputCacheName;
  protected String intermediateCacheName;
  protected LeadsMapper<?, ?, ?, ?> mapper;
  protected LeadsCollector<?, ?> collector;
  protected LeadsReducer<?,?> reducer;
  protected String uuid;


  public MapReduceOperator(Node com, InfinispanManager persistence, LogProxy log, Action action) {

    super(com,persistence,log,action);
    inputCacheName = getInput();
    outputCacheName = action.getData().getObject("operator").getString("id");
    intermediateCacheName = action.getData().getObject("operator").getString("id")+".intermediate";
    uuid = UUID.randomUUID().toString();
  }

  public void setMapper(LeadsMapper<?, ?, ?, ?> mapper) {
    this.mapper = mapper;

  }

  public void setReducer(LeadsReducer<?, ?> reducer) {
    this.reducer = reducer;
  }

  @Override
  public void init(JsonObject config) {
    conf.putString("output",getOutput());
    inputCache = (Cache) manager.getPersisentCache(inputCacheName);
    intermediateCache = (BasicCache) manager.getPersisentCache(intermediateCacheName);
    //create Intermediate cache name for data on the same Sites as outputCache
    intermediateDataCache = (BasicCache) manager.getPersisentCache(intermediateCacheName+".data");
    //create Intermediate  keys cache name for data on the same Sites as outputCache;
    keysCache = (BasicCache)manager.getPersisentCache(intermediateCacheName+".keys");
    //createIndexCache for getting all the nodes that contain values with the same key! in a mc
    indexSiteCache = (BasicCache)manager.getPersisentCache(intermediateCacheName+".indexed");
//    indexSiteCache = (BasicCache)manager.getIndexedPersistentCache(intermediateCacheName+".indexed");
    outputCache = (BasicCache) manager.getPersisentCache(outputCacheName);
    reduceInputCache = (Cache) keysCache;
    collector = new LeadsCollector(0, intermediateCacheName);
  }


  @Override /// Example do not use
  public void execute() {
    super.start();
  }

  @Override
  public void cleanup() {
    super.cleanup();
    if(executeOnlyReduce) {
      intermediateCache.stop();
      indexSiteCache.stop();
      intermediateDataCache.stop();
      keysCache.stop();
    }
  }

  @Override
  public boolean isSingleStage() {
    return false;
  }
  @Override
  public void createCaches(boolean isRemote, boolean executeOnlyMap, boolean executeOnlyReduce) {
    Set<String> targetMC = getTargetMC();
    for(String mc : targetMC){
      createCache(mc,getOutput());
      createCache(mc, intermediateCacheName);
      //create Intermediate cache name for data on the same Sites as outputCache
      createCache(mc,intermediateCacheName+".data");
      //create Intermediate  keys cache name for data on the same Sites as outputCache;
      createCache(mc,intermediateCacheName+".keys");
      //createIndexCache for getting all the nodes that contain values with the same key! in a mc
      createCache(mc,intermediateCacheName+".indexed");
//    indexSiteCache = (BasicCache)manager.getIndexedPersistentCache(intermediateCacheName+".indexed");

    }
  }
  @Override
  public void setupMapCallable(){
//    conf.putString("output",getOutput());
    inputCache = (Cache) manager.getPersisentCache(inputCacheName);
//    intermediateCache = (BasicCache) manager.getPersisentCache(intermediateCacheName);
    //create Intermediate cache name for data on the same Sites as outputCache
//    intermediateDataCache = (BasicCache) manager.getPersisentCache(intermediateCacheName+".data");
    //create Intermediate  keys cache name for data on the same Sites as outputCache;
//    keysCache = (BasicCache)manager.getPersisentCache(intermediateCacheName+".keys");
    //createIndexCache for getting all the nodes that contain values with the same key! in a mc
//    indexSiteCache = (BasicCache)manager.getPersisentCache(intermediateCacheName+".indexed");
    //    indexSiteCache = (BasicCache)manager.getIndexedPersistentCache(intermediateCacheName+".indexed");
//    outputCache = (BasicCache) manager.getPersisentCache(outputCacheName);
//    reduceInputCache = (Cache) keysCache;
    collector = new LeadsCollector(0, intermediateCacheName);
    mapperCallable = new LeadsMapperCallable((Cache) inputCache,collector,mapper,
                                   LQPConfiguration.getInstance().getMicroClusterName());
  }
  @Override
  public void setupReduceCallable(){
    conf.putString("output",getOutput());
    intermediateCache = (BasicCache) manager.getPersisentCache(intermediateCacheName);
    log.error("ReducerIntermediate " + intermediateCache.size());
    //create Intermediate cache name for data on the same Sites as outputCache
    intermediateDataCache = (BasicCache) manager.getPersisentCache(intermediateCacheName+".data");
    log.error("ReducerIntermediateData " + intermediateDataCache.size());
    //create Intermediate  keys cache name for data on the same Sites as outputCache;
    keysCache = (BasicCache)manager.getPersisentCache(intermediateCacheName+".keys");
    log.error("ReducerIntermediateKeys " + keysCache.size());
    //createIndexCache for getting all the nodes that contain values with the same key! in a mc
    indexSiteCache = (BasicCache)manager.getPersisentCache(intermediateCacheName+".indexed");
    //    indexSiteCache = (BasicCache)manager.getIndexedPersistentCache(intermediateCacheName+".indexed");
    log.error("ReducerIntermediateSite " + indexSiteCache.size());
    outputCache = (BasicCache) manager.getPersisentCache(outputCacheName);
    reduceInputCache = (Cache) keysCache;
    collector = new LeadsCollector(0, outputCache.getName());
    inputCache = (Cache) keysCache;
    reducerCallable =  new LeadsReducerCallable(outputCache.getName(), reducer,
                                                                         intermediateCacheName);
  }
}
