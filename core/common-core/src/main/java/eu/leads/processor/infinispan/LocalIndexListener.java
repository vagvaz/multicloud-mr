package eu.leads.processor.infinispan;

import eu.leads.processor.common.LeadsListener;
import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.continuous.ConcurrentDiskQueue;
import eu.leads.processor.common.continuous.EventTriplet;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.LevelDBIndex;
import eu.leads.processor.plugins.EventType;
import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by vagvaz on 16/07/15.
 */
@Listener(sync = true, primaryOnly = true, clustered = false) public class LocalIndexListener implements LeadsListener,Runnable {
  private String confString;
  transient private volatile Object mutex;
  String cacheName;
  //    transient LevelDBIndex index;
  //    transient List<LevelDBIndex> indexes;
  transient ConcurrentDiskQueue queue;
  transient Thread thread;
  transient List<LevelDBIndex> indexes;
  transient Cache targetCache;
  transient Cache keysCache;
  transient Cache dataCache;
  transient Logger log;
  transient ProfileEvent pevent;
  private int parallelism = 1;
  private boolean isDirty = false;
  private boolean flush = false;

  public LocalIndexListener(InfinispanManager manager, String cacheName) {
    this.cacheName = cacheName;
  }

  public String getCacheName() {
    return cacheName;
  }

  public void setCacheName(String cacheName) {
    this.cacheName = cacheName;
  }

  //    public LevelDBIndex getIndex(int i) {
  //        isDirty = true;
  //        return indexes.get(i);
  //    }

  public LevelDBIndex getIndex(int i) {
    isDirty = true;
    return indexes.get(i);
  }


  //    public void setIndex(LevelDBIndex index,int i) {
  //        this.indexes.set(i,index);
  //    }
  public List<LevelDBIndex> getIndexes() {
    return indexes;
  }

  public Cache getKeysCache() {
    return keysCache;
  }

  public void setKeysCache(Cache keysCache) {
    this.keysCache = keysCache;
  }

  public Cache getDataCache() {
    return dataCache;
  }

  public void setDataCache(Cache dataCache) {
    this.dataCache = dataCache;
  }

  @CacheEntryCreated public void created(CacheEntryCreatedEvent event) {
    if (isDirty) {
      System.err.println("DIRTY === ");
      System.exit(-1);
    }
    if (event.isPre()) {
      return;
    }

    //        if(event.getKey() instanceof ComplexIntermediateKey) {
    //        pevent.start("IndexPut");
    processEvent(event.getKey(),event.getValue());
    //        pevent.end();
    //        targetCache.removeAsync(event.getKey());
    //            synchronized (mutex){
    //                mutex.notifyAll();
    //            }
    //        }

  }

  private void processEvent(Object key,Object value) {
    EventTriplet e = new EventTriplet(EventType.CREATED,key,value);
    queue.add(e);
    synchronized (mutex) {
      mutex.notify();
    }
  }

  @CacheEntryModified public void modified(CacheEntryModifiedEvent event) {
    if (isDirty) {
      System.err.println("DIRTY ++++++=== ");
      System.exit(-1);
    }
    if (event.isPre()) {
      //            ComplexIntermediateKey key = (ComplexIntermediateKey) event.getKey();
      //            System.err.println("PREKey modified " + event.getKey() + " key "  + key.getKey() + " " + key.getNode() + " " + key.getSite() + " " + key.getCounter());
      return;
    }
    //        System.err.println("localmodify " + event.isOriginLocal() + " " + event.isCommandRetried() + " " + event.isCreated() + " " + event.isPre());
    //        log.error("orig " + event.isOriginLocal() + " ret " + event.isCommandRetried() + " crea " + event.isCreated() + " pre  " + event.isPre());

    //        pevent.start("IndexPut");
    //        if(event.getKey() instanceof ComplexIntermediateKey) {
    processEvent(event.getKey(),event.getValue());

    //        pevent.end();
    //            synchronized (mutex){
    //                mutex.notifyAll();
    //            }
    //        }
  }

  @Override public InfinispanManager getManager() {
    return null;
  }

  @Override public void setManager(InfinispanManager manager) {

  }

  @Override public void initialize(InfinispanManager manager, JsonObject conf) {
    mutex = new Object();
    this.targetCache = (Cache) manager.getPersisentCache(cacheName);
    System.err.println("Listener target Cache = " + targetCache.getName());
    //        this.keysCache = manager.getLocalCache(cacheName+".index.keys");
    //        this.dataCache = manager.getLocalCache(cacheName+".index.data");
    //        this.index = new IntermediateKeyIndex(keysCache,dataCache);
    queue = new ConcurrentDiskQueue(500);

    Thread thread = new Thread(this);
    parallelism = LQPConfiguration.getInstance().getConfiguration().getInt("node.engine.parallelism", 4);
    indexes = new ArrayList<>(parallelism);
    for (int i = 0; i < parallelism; i++) {
      indexes.add(new LevelDBIndex(
          System.getProperties().getProperty("java.io.tmpdir") + "/" + StringConstants.TMPPREFIX + "/interm-index/"
              + InfinispanClusterSingleton.getInstance().getManager().getUniquePath() + "/" + cacheName + i,
          cacheName + ".index-" + i));
    }

    //        this.index = new LevelDBIndex( System.getProperties().getProperty("java.io.tmpdir")+"/"+StringConstants.TMPPREFIX+"/interm-index/"+ InfinispanClusterSingleton.getInstance().getManager().getUniquePath()+"/"+cacheName,cacheName+".index");
    log = LoggerFactory.getLogger(LocalIndexListener.class);
    pevent = new ProfileEvent("indexPut", log);
    thread.start();
  }

  @Override public void initialize(InfinispanManager manager) {
    initialize(manager, null);
  }

  @Override public String getId() {
    return this.getClass().toString();
  }

  @Override public void close() {
    for (LevelDBIndex index : indexes) {
      index.flush();
      index.close();
    }
    flush = true;
    indexes.clear();
    if (keysCache != null) {
      keysCache.clear();
      ;
      keysCache.stop();
      keysCache.getCacheManager().removeCache(keysCache.getName());
    }
    if (dataCache != null) {
      dataCache.clear();
      dataCache.stop();
      dataCache.getCacheManager().removeCache(dataCache.getName());
    }
    keysCache = null;
    dataCache = null;
  }

  @Override public void setConfString(String s) {
    confString = s;
  }

  void waitForAllData() {
    System.err.println("get the size of target");
    if(flush){
      return;
    }
    synchronized (mutex) {
      flush = true;
      mutex.notify();
    }
    for (LevelDBIndex index : indexes) {
      index.flush();
    }

  }

  @Override public void run() {
    while(!flush){
      EventTriplet e = (EventTriplet) queue.poll();
      if(e == null){
        synchronized (mutex){
          try {
            mutex.wait();
            continue;
          } catch (InterruptedException e1) {
            e1.printStackTrace();
          }
        }
      }
      ComplexIntermediateKey key = (ComplexIntermediateKey) e.getKey();
      int indx = Math.abs(key.hashCode()) % parallelism;
      indexes.get(indx).put(key.getKey(), e.getValue());
      if(flush) {
        EventTriplet triplet = (EventTriplet) queue.poll();
        while (triplet != null) {
           key = (ComplexIntermediateKey) triplet.getKey();
           indx = Math.abs(key.hashCode()) % parallelism;
          indexes.get(indx).put(key.getKey(), triplet.getValue());
        }
        flush = false;
        for (LevelDBIndex index : indexes) {
          index.flush();
        }
        synchronized (mutex) {
          mutex.notify();
        }
      }
    }

  }
}
