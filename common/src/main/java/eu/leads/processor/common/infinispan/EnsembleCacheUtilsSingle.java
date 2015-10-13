package eu.leads.processor.common.infinispan;

import com.sun.rmi.rmid.ExecPermission;
import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.ComplexIntermediateKey;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.Site;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.infinispan.ensemble.cache.distributed.HashBasedPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by vagvaz on 10/10/15.
 */
public class EnsembleCacheUtilsSingle {

  private static Logger log = LoggerFactory.getLogger(EnsembleCacheUtilsSingle.class);
  private  boolean useAsync;

  private  volatile Object mutex = new Object();
  private  Boolean initialized = false;
  private  int batchSize = 20;
  private  long threadCounter = 0;
  private  long threadBatch = 3;
  private    ThreadPoolExecutor auxExecutor;
  //    private static volatile Object runnableMutex = new Object();
  private  ConcurrentHashMap<String, Map<String,TupleBuffer>> microclouds;
  //    private static ConcurrentHashMap<String, List<BatchPutAllAsyncThread>> microcloudThreads;
  private  HashBasedPartitioner partitioner;
  private  ConcurrentMap<String,EnsembleCacheManager> ensembleManagers;
  private  InfinispanManager localManager;
  private  String localMC;
  private  ThreadPoolExecutor batchPutExecutor;
  private  int totalBatchPutThreads =16;
  private  String ensembleString ="";
  private  ConcurrentLinkedQueue<NotifyingFuture> localFutures;
  private  int localBatchSize =10;
  private ConcurrentLinkedDeque<BatchPutRunnable > microcloudRunnables;
  private ConcurrentLinkedDeque<SyncPutRunnable > runnables;


  public  EnsembleCacheUtilsSingle() {
    //    synchronized (mutex) {
    //      if (initialized) {
    //        return;
    //      }

    //Initialize auxiliary put
    useAsync = LQPConfiguration.getInstance().getConfiguration()
        .getBoolean("node.infinispan.putasync", true);
    log.info("Using asynchronous put " + useAsync);
    //            concurrentQuue = new ConcurrentLinkedQueue<>();
    batchSize = LQPConfiguration.getInstance().getConfiguration()
        .getInt("node.ensemble.batchsize", 100);
    threadBatch = LQPConfiguration.getInstance().getConfiguration().getInt(
        "node.ensemble.threads", 3);


    auxExecutor = new ThreadPoolExecutor((int)threadBatch,(int)(threadBatch),1000, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>());
    runnables = new ConcurrentLinkedDeque<>();
    for (int i = 0; i < 10 * (threadBatch); i++) {
      runnables.add(new SyncPutRunnable(this));
    }
    initialized = true;

    //Initialize BatchPut Structures
    totalBatchPutThreads = LQPConfiguration.getInstance().getConfiguration().getInt("node.ensemble.batchput.threads",4);
    localBatchSize = LQPConfiguration.getInstance().getConfiguration().getInt("node.ensemble.batchput.batchsize",localBatchSize);
    System.err.println("threads " + threadBatch + " batchSize " + batchSize + " async = " + useAsync +" batchPutThreads " + totalBatchPutThreads + " localBatch " + localBatchSize);
    batchPutExecutor = new ThreadPoolExecutor(totalBatchPutThreads,totalBatchPutThreads,2000,TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>());
    microclouds = new ConcurrentHashMap<>();
    //            microcloudThreads  = new ConcurrentHashMap<>();
    microcloudRunnables = new ConcurrentLinkedDeque<>();
    for (int index = 0; index < 10*totalBatchPutThreads; index++){
      microcloudRunnables.add(new BatchPutRunnable(3,this));
    }
    ensembleManagers = new ConcurrentHashMap<>();
    partitioner = null;
    localManager = null;
    localMC =null;
    localFutures = new ConcurrentLinkedQueue<NotifyingFuture>();
    //    }
  }

  public  void clean() throws ExecutionException, InterruptedException {
    waitForAllPuts();
    localFutures.clear();
    for(Map.Entry<String,Map<String,TupleBuffer>> mc : microclouds.entrySet()) {
      mc.getValue().clear();
      partitioner = null;
    }

  }
  public  void initialize(EnsembleCacheManager manager){
    initialize(manager, true);
  }

  public  void initialize(EnsembleCacheManager manager, boolean isEmbedded) {
    synchronized (mutex) {
      ensembleString = "";
      ArrayList<EnsembleCache> cachesList = new ArrayList<>();

      for (Object s : new ArrayList<>(manager.sites())) {
        Site site = (Site) s;
        ensembleString += site.getName();
        cachesList.add(site.getCache());
        EnsembleCacheManager emanager = ensembleManagers.get(site.getName());
        if (emanager == null) {
          emanager = new EnsembleCacheManager(site.getName());
          ensembleManagers.put(site.getName(), emanager);
          Map<String,TupleBuffer> newMap = new ConcurrentHashMap<>();
          microclouds.putIfAbsent(site.getName(), newMap);
        }
      }
      if(localManager == null && isEmbedded){
        localManager = InfinispanClusterSingleton.getInstance().getManager();
        if(localMC == null)
          localMC = resolveMCName() + "L";
      }

      partitioner = new HashBasedPartitioner(cachesList);
    }
  }


  private  String resolveMCName() {
    String result = "";
    String externalIp = LQPConfiguration.getInstance().getConfiguration().getString(
        StringConstants.PUBLIC_IP);
    if(externalIp == null){
      externalIp = LQPConfiguration.getInstance().getConfiguration().getString("node.ip");
    }
    boolean hasport = false;
    if(externalIp.contains(":")){
      String candidatePort = externalIp.substring(externalIp.lastIndexOf(":")+1,externalIp.length());
      try {
        Integer port = Integer.valueOf(candidatePort);
        hasport = true;
      }catch(Exception e){
        hasport =false;
      }
    }
    if(!hasport){
      externalIp+=":";
    }

    for(String ensemble : ensembleManagers.keySet()) {
      if(ensemble.contains(externalIp)){
        result = ensemble;
        break;
      }
    }

    return result;
  }

  private  String decideMC(String keyString) {
    EnsembleCache cache = partitioner.locate(keyString);
    String result = "";
    for(Object s : cache.sites()){
      Site site = (Site)s;
      result = site.getName();
    }
    return result;
  }
  public   SyncPutRunnable getRunnable(){
    SyncPutRunnable result = null;
    //    System.err.println("GET aux run " + runnables.size());
    result = runnables.poll();
    while(result == null){
      try {
        Thread.sleep(0, 500000);
        //                    Thread.yield();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      result = runnables.poll();
      //            }
    }

    return result;
  }

  public  BatchPutRunnable getBatchPutRunnable(){
    BatchPutRunnable result = null;
    //        synchronized (runnableMutex){
     System.err.println("GET batch run " + microcloudRunnables.size());
    result = microcloudRunnables.poll();
    while(result == null){

      try {
        Thread.sleep(0,500000);
        //                    Thread.yield();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      result = microcloudRunnables.poll();
      //            }
    }

    return result;
  }


  public  void addRunnable(SyncPutRunnable runnable){
    runnables.add(runnable);
    //    System.err.println("add aux run " + runnables.size());
  }

  public  void addBatchPutRunnable(BatchPutRunnable runnable)
  {
    System.err.println("add microcloud run " + microcloudRunnables.size());
    microcloudRunnables.add(runnable);
  }


  public void waitForLocalPuts(){
    while(!localFutures.isEmpty()){
      removeCompleted();
      try {
        Thread.sleep(0,100000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Thread.yield();
    }
  }
  public  void waitForAuxPuts() throws InterruptedException {
    System.err.println("WaitForAuxPuts");
    while(runnables.size() != 10*(threadBatch)) {
      try {
        //            auxExecutor.awaitTermination(100,TimeUnit.MILLISECONDS);
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
        PrintUtilities.logStackTrace(log, e.getStackTrace());
        throw e;
      }
    }
    System.err.println("WaitForAuxPuts---end");
  }
  public void waitForAllPuts() throws InterruptedException, ExecutionException {
    System.err.println("wait for puts");
    for(Map.Entry<String,Map<String,TupleBuffer>> mc : microclouds.entrySet()) {
      for (Map.Entry<String, TupleBuffer> cache : mc.getValue().entrySet()) {
        if(!mc.getKey().equals(localMC) ) {
          BatchPutRunnable runnable = getBatchPutRunnable();
          runnable.setBuffer(cache.getValue());
          batchPutExecutor.submit(runnable);
        }
        //                else{
        //                    cache.getValue().flushToMC();
        //     vagvaz           }
        else{
          if(cache.getValue().getBuffer().size() > 0) {
            Cache localCache = (Cache) localManager.getPersisentCache(cache.getKey());
            cache.getValue().flushToLocalCache();
            //                        cache.getValue().release();
          }
        }
      }
    }
    //flush remotely batchputlisteners
    for(Map.Entry<String,Map<String,TupleBuffer>> mc : microclouds.entrySet()){
      System.err.println("localMC: " + localMC + " mc " + mc.getKey());
      for(Map.Entry<String,TupleBuffer> cache : mc.getValue().entrySet()){
        if(!mc.getKey().equals(localMC)) {
          cache.getValue().flushEndToMC();
          //                    cache.getValue().flushEndToMC();
          //                    cache.getValue().flushEndToMC();
        }
        //                else{//vagvaz
        //                    cache.getValue().flushEndToMC();
        //                }
      }
    }
    System.err.println("Wait batchput");
    while( microcloudRunnables.size() !=  10*totalBatchPutThreads){
      try {
        //            auxExecutor.awaitTermination(100,TimeUnit.MILLISECONDS);
        System.err.println("microRunna " + microcloudRunnables.size() + " instead of " + (10*totalBatchPutThreads));
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        PrintUtilities.logStackTrace(log,e.getStackTrace()); throw e;
      }
    }
    System.err.println("Wait batchput---end");
    waitForAuxPuts();

    System.err.println("local wait " + localFutures.size());
    for(NotifyingFuture future : localFutures)
    {
      try {
        if(future != null)
          future.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
        PrintUtilities.logStackTrace(log,e.getStackTrace()); throw e;
      } catch (ExecutionException e) {
        e.printStackTrace();
        PrintUtilities.logStackTrace(log,e.getStackTrace()); throw e;
      }
    }
    localFutures.clear();
    System.err.println("ensembesingle wait end");
  }

  public void addLocalFuture(NotifyingFuture future){
    localFutures.add(future);
  }

  public void removeCompleted(){
    Iterator<NotifyingFuture> it = localFutures.iterator();
    while(it.hasNext()){
      NotifyingFuture f = it.next();
      if(f.isDone()){
        it.remove();
      }
    }
  }
  public  void putToCache(BasicCache cache, Object key, Object value) {
    batchPutToCache(cache, key, value,true);
  }

  private  void batchPutToCache(BasicCache cache, Object key, Object value, boolean b) {
    if( !((key instanceof String )|| (key instanceof ComplexIntermediateKey)) || !(value instanceof Tuple)){
      putToCacheDirect(cache, key, value);
    }
    if(b){
      if(cache instanceof EnsembleCache){
        String mc = decideMC(key.toString());
        if(mc.equals(localMC)){
          //vagvaz                    putToMC(cache,key,value,localMC);
          putToLocalMC(cache,key,value);
        }
        else{
          putToMC(cache,key,value,mc);
        }
      }
      else{
        putToLocalMC(cache,key,value);
        //                putToMC(cache,key,value,localMC);
      }
    }
    else{
      putToCacheDirect(cache, key, value);
    }
  }

  private  void putToMC(BasicCache cache, Object key, Object value, String mc) {
    if(mc == null || mc.equals("")){
      log.error("Cache is of type " + cache.getClass().toString() + " but mc is " + mc + " using direct put");
      putToCacheDirect(cache,key,value);
      return;
    }
    Map<String,TupleBuffer> buffer = microclouds.get(mc);

    if(buffer == null) {
      microclouds.put(mc, new ConcurrentHashMap<String, TupleBuffer>());
    }
    TupleBuffer tupleBuffer = buffer.get(cache.getName());
    if(tupleBuffer == null){
      tupleBuffer= new TupleBuffer(batchSize,cache.getName(),ensembleManagers.get(mc),mc,this);
      microclouds.get(mc).put(cache.getName(),tupleBuffer);
    }
    if(tupleBuffer.getCacheName()==null)
    {
      tupleBuffer.setCacheName(cache.getName());
    }
    if(tupleBuffer.add(key, value)){
      BatchPutRunnable runnable = getBatchPutRunnable();
      runnable.setBuffer(tupleBuffer);
      batchPutExecutor.submit(runnable);
    }
  }

  private  void putToLocalMC(BasicCache cache, Object key, Object value) {
    if(localMC == null){
      log.error("Cache is of type " + cache.getClass().toString() + " but localMC is " + localMC + " using direct put");
      putToCacheDirect(cache, key, value);
      return;
    }
    //    Map<String,TupleBuffer> mcBufferMap = microclouds.get(localMC);
    //
    //    if(mcBufferMap == null) { // create buffer map for localMC
    //      microclouds.put(localMC, new ConcurrentHashMap<String, TupleBuffer>());
    //    }
    //    Cache localCache = (Cache) InfinispanClusterSingleton.getInstance().getManager().getPersisentCache(cache.getName());
    //    TupleBuffer tupleBuffer = mcBufferMap.get(cache.getName());
    //    if(tupleBuffer == null){
    //      tupleBuffer= new TupleBuffer(batchSize,localCache,ensembleManagers.get(localMC));
    //      microclouds.get(localMC).put(cache.getName(),tupleBuffer);
    //    }
    //    if(tupleBuffer.getCacheName()==null)
    //    {
    //      tupleBuffer.setCacheName(cache.getName());
    //    }
    //    if(tupleBuffer.add(key, value)){
    //      tupleBuffer.flushToLocalCache();
    //    }
    Cache localCache =
        (Cache) localManager.getPersisentCache(  cache.getName());
    putToCacheDirect(localCache, key, value);
  }

  public  void putToCacheDirect(BasicCache cache,Object key,Object value){

    if (useAsync) {
      putToCacheAsync(cache, key, value);
      return;
    }
    putToCacheSync(cache, key, value);
  }

  private  void putToCacheSync(BasicCache cache, Object key, Object value) {
    //        profExecute.start("putToCache Sync");
    boolean isok = false;
    while (!isok) {
      try {
        if (cache != null) {
          if (key == null || value == null) {
            log.error(
                "SERIOUS PROBLEM with key/value null key: " + (key == null) + " value "
                    + (value == null));
            if (key != null) {
              log.error("key " + key.toString());
            }
            if (value != null) {
              log.error("value: " + value);
            }
            isok = true;
            continue;
          }
          cache.put(key, value);

          //              log.error("Successful " + key);
          isok = true;
        } else {
          log.error("CACHE IS NULL IN PUT TO CACHE for " + key.toString() + " " + value
              .toString());
          isok = true;
        }
      } catch (Exception e) {
        isok = false;

        log.error("PUT TO CACHE " + e.getMessage() + " " + key);
        log.error("key " + (key == null) + " value " + (value == null) + " cache " + (cache
            == null) + " log " + (log == null));

        System.err.println("PUT TO CACHE " + e.getMessage());
        e.printStackTrace();
        if (e.getMessage().startsWith("Cannot perform operations on ")) {
          e.printStackTrace();
          System.exit(-1);
        }
      }
    }
    //        profExecute.end();
  }

  private void putToCacheAsync(final BasicCache cache, final Object key, final Object value) {
    //        counter = (counter + 1) % Long.MAX_VALUE;
    //        profExecute.start("putToCache Async");
    boolean isok = false;
    while (!isok) {
      try {
        if (cache != null) {
          if (key == null || value == null) {
            log.error(
                "SERIOUS PROBLEM with key/value null key: " + (key == null) + " value "
                    + (value == null));
            if (key != null) {
              log.error("key " + key.toString());
            }
            if (value != null) {
              log.error("value: " + value);
            }
            isok = true;
            continue;
          }
          SyncPutRunnable putRunnable = getRunnable();
          putRunnable.setParameters(cache,key,value);
          auxExecutor.submit(putRunnable);
          isok = true;
        } else {
          log.error("CACHE IS NULL IN PUT TO CACHE for " + key.toString() + " " + value
              .toString());
          isok = true;
        }
      } catch (Exception e) {
        isok = false;
        if(e instanceof RejectedExecutionException){
          try {
            Thread.sleep(10);
          } catch (InterruptedException e1) {
            e1.printStackTrace();
          }
          continue;
        }
        log.error("PUT TO CACHE " + e.getMessage() + " " + key);
        log.error("key " + (key == null) + " value " + (value == null) + " cache " + (cache
            == null) + " log " + (log == null));

        System.err.println("PUT TO CACHE " + e.getMessage());
        e.printStackTrace();
        if (e.getMessage().startsWith("Cannot perform operations on ")) {
          e.printStackTrace();
          System.exit(-1);
        }
      }
    }
  }

  public  <KOut> void putIfAbsentToCache(BasicCache cache, KOut key, KOut value) {
    putToCache(cache, key, value);
  }

  public  void removeCache(String cacheName) {
    synchronized (mutex) {
      for (Map.Entry<String, Map<String, TupleBuffer>> entry : microclouds.entrySet()) {
        Iterator iterator = entry.getValue().entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<String, TupleBuffer> buffer = (Map.Entry<String, TupleBuffer>) iterator.next();
          if (buffer.getValue().getCacheName().equals(cacheName)) {
            buffer.getValue().release();
            iterator.remove();
          }
        }
      }
    }
  }

  @Override
  public void finalize(){
    //    auxExecutor.shutdownNow();
    //    batchPutExecutor.shutdownNow();
    System.err.println("Finalize ensmeblCacheUtilsSingle");
  }
}
