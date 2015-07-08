package eu.leads.processor.common.infinispan;

import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.conf.LQPConfiguration;
import org.infinispan.commons.api.BasicCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by vagvaz on 3/7/15.
 */
public class EnsembleCacheUtils {
  static Logger profilerLog = LoggerFactory.getLogger("###PROF###" + EnsembleCacheUtils.class);
  static ProfileEvent profExecute =
      new ProfileEvent("Execute " + EnsembleCacheUtils.class, profilerLog);
  static Logger log = LoggerFactory.getLogger(EnsembleCacheUtils.class);
  static boolean useAsync;
  //    static Queue<NotifyingFuture<Void>> concurrentQuue;
  static Map<String, BasicCache> currentCaches;
  static Map<String, Map<Object, Object>> mapsToPut;
  static Queue<Thread> threads;
  static volatile Object mutex = new Object();
  static Boolean initialized = false;
  static int batchSize = 20;
  static long counter = 0;
  static long threadCounter = 0;
  static long threadBatch = 3;

  public static void initialize() {
    synchronized (mutex) {
      if (initialized) {
        return;
      }
      useAsync = LQPConfiguration.getInstance().getConfiguration()
          .getBoolean("node.infinispan.putasync", true);
      log.info("Using asynchronous put " + useAsync);
      //            concurrentQuue = new ConcurrentLinkedQueue<>();
      threads = new ConcurrentLinkedQueue<>();
      //            ccr = new ClearCompletedRunnable(concurrentQuue,mutex,threads);
      initialized = true;
      batchSize = LQPConfiguration.getInstance().getConfiguration()
          .getInt("node.ensemble.batchsize", 10);
      threadBatch = LQPConfiguration.getInstance().getConfiguration().getInt("node.ensemble.threads",2);
      currentCaches = new ConcurrentHashMap<>();
      mapsToPut = new ConcurrentHashMap<>();
    }
  }

  public static void waitForAllPuts() {
    //        profExecute.start("waitForAllPuts");
    clearCompleted();
    while (!threads.isEmpty()) {
      Thread t = threads.poll();
      try {
        t.join();
      } catch (InterruptedException e) {
        log.error("EnsembleCacheUtils waitForAllPuts Wait clean threads " + e.getClass()
            .toString());
        log.error(e.getStackTrace().toString());
        e.printStackTrace();
      }
    }
    //        profExecute.end();
  }

  public static void putToCache(BasicCache cache, Object key, Object value) {
    if (useAsync) {
      putToCacheAsync(cache, key, value);
      if (counter % batchSize == 0) {
        clearCompleted();
      }
      return;
    }
    putToCacheSync(cache, key, value);
  }

  private static void clearCompleted() {
    Map<String,BasicCache> caches;
    Map<String,Map<Object,Object>> objects;
    List<Thread> completedThreads = new LinkedList<>();
    synchronized (mutex) {
      threadCounter = threads.size();
//      System.err.println("Active threads: " + threadCounter);
      if(threadCounter > threadBatch){
        for(Thread t : threads){
          if(!t.isAlive()){
            completedThreads.add(t);
          }

        }
//        System.err.println("Completed threads: " + completedThreads.size());
        while(threads.size() - completedThreads.size() > threadBatch) {
          for (Thread t : threads) {
            try {
              t.join();
              if (!t.isAlive()) {
                completedThreads.add(t);
                if(threads.size() - completedThreads.size() < threadBatch) {
//                 System.out.println("t " + threads.size() + "  c " + completedThreads.size() + " so " + (threads.size() - completedThreads.size() < threadBatch));
                  break;
                }
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
              log.error("EnsembleCacheUtilsClearCompletedException " + e.getMessage());
              PrintUtilities.logStackTrace(log, e.getStackTrace());
            }
          }
        }
      }
      for(Thread t : completedThreads){
        threads.remove(t);
      }
//      System.err.println("After cleanup Active threads: " + threads.size());
      assert (threads.size() < threadBatch);
      caches = currentCaches;
      objects = mapsToPut;

      currentCaches = new ConcurrentHashMap<>();
      mapsToPut = new ConcurrentHashMap<>();
      BatchPutAllAsyncThread batchPutAllAsyncThread = new BatchPutAllAsyncThread(caches,objects);
      threads.add(batchPutAllAsyncThread);
      batchPutAllAsyncThread.start();
    }
  }

  private static void putToCacheSync(BasicCache cache, Object key, Object value) {
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

  private static void putToCacheAsync(BasicCache cache, Object key, Object value) {
    counter = (counter + 1) % Long.MAX_VALUE;
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
          //                    NotifyingFuture fut = cache.putAsync(key, value);
          //                    concurrentQuue.add(fut);
          BasicCache currentCache = currentCaches.get(cache.getName());
          if (currentCache == null) {
            synchronized (mutex) {
              currentCaches.put(cache.getName(), cache);

              Map<Object, Object> newMap = new ConcurrentHashMap<>();
              newMap.put(key, value);
              mapsToPut.put(cache.getName(), newMap);
            }
          } else {
            Map<Object, Object> cacheMap = mapsToPut.get(cache.getName());
            if(cacheMap == null){
              synchronized (mutex){
                cacheMap = new ConcurrentHashMap<>();
                mapsToPut.put(cache.getName(),cacheMap);
              }
            }
            cacheMap.put(key, value);
          }

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

  public static <KOut> void putIfAbsentToCache(BasicCache cache, KOut key, KOut value) {
    putToCache(cache, key, value);
    //    boolean isok = false;
    //    while(!isok) {
    //      try {
    //        cache.put(key, value);
    //        isok =true;
    //      } catch (Exception e) {
    //        isok = false;
    //        System.err.println("PUT TO CACHE " + e.getMessage());
    //      }
    //    }
  }
}
