package eu.leads.processor.infinispan;

import eu.leads.processor.common.infinispan.BatchPutListener;
import eu.leads.processor.common.infinispan.ClusterInfinispanManager;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.EngineUtils;
import eu.leads.processor.math.FilterOpType;
import eu.leads.processor.math.FilterOperatorNode;
import eu.leads.processor.math.FilterOperatorTree;
import eu.leads.processor.math.MathUtils;
import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.QueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Created by vagvaz on 2/18/15.
 */
public abstract class LeadsBaseCallable<K, V> implements LeadsCallable<K, V>,

    DistributedCallable<K, V, String>, Serializable {
  protected String configString;
  protected String output;
  transient protected JsonObject conf = null;
  transient protected boolean isInitialized = false;
  transient protected EmbeddedCacheManager embeddedCacheManager = null;
  transient protected InfinispanManager imanager = null;
  transient protected Set<K> keys = null;
  transient protected Cache<K, V> inputCache = null;
  transient protected EnsembleCache outputCache = null;
  protected String ensembleHost;
  transient protected Object luceneKeys = null;
  transient protected HashMap<String, Cache> indexCaches = null;
  transient protected FilterOperatorTree tree = null;
  transient protected List<LeadsBaseCallable> callables;
  transient protected List<ExecuteRunnable> executeRunnables;
  transient Queue<Map.Entry<K, V>> input;
  protected int callableIndex = -1;
  protected int callableParallelism = 1;
  protected boolean continueRunning = true;
  //  transient protected EnsembleCacheUtilsSingle ensembleCacheUtilsSingle;
  long start = 0;
  long end = 0;
  int readCounter = 0;

  //  transient protected RemoteCache outputCache;
  //  transient protected RemoteCache ecache;
  //  transient protected RemoteCacheManager emanager;
  transient protected EnsembleCacheManager emanager;
  transient protected EnsembleCache ecache;
  transient Logger profilerLog;
  protected ProfileEvent profCallable;
  protected LeadsCollector collector;

  public LeadsBaseCallable() {
    callableParallelism = LQPConfiguration.getInstance().getConfiguration().getInt("node.engine.parallelism", 4);
    callableIndex = -2;
  }

  public LeadsBaseCallable(String configString, String output) {
    this.configString = configString;
    this.output = output;
    profilerLog = LoggerFactory.getLogger("###PROF###" + this.getClass().toString());
    profCallable = new ProfileEvent("Callable Construct" + this.getClass().toString(), profilerLog);
    callableIndex = -1;
    callableParallelism = LQPConfiguration.getInstance().getConfiguration().getInt("node.engine.parallelism", 4);
  }

  public LeadsBaseCallable copy() {
    LeadsBaseCallable result = null;
    try {
      Constructor<?> constructor = this.getClass().getConstructor();
      result = (LeadsBaseCallable) constructor.newInstance();
      result.setCollector(new LeadsCollector(collector.getMaxCollectorSize(), collector.getCacheName()));
      result.setEnsembleHost(ensembleHost);
      result.setOutput(output);
      result.setConfigString(configString);
      if (result instanceof LeadsMapperCallable) {
        LeadsMapperCallable mapperCallable = (LeadsMapperCallable) result;
        LeadsMapperCallable thisCallable = (LeadsMapperCallable) this;
        mapperCallable.setSite(thisCallable.getSite());
        LeadsCombiner thisCombiner = thisCallable.getCombiner();
        if ( thisCombiner != null) {
          mapperCallable.setCombiner(thisCombiner);
//          thisCallable.getCollector().setUseCombiner(true);
        }
        mapperCallable.setMapper(thisCallable.getMapper());
      } else if (result instanceof LeadsLocalReducerCallable) {
        LeadsLocalReducerCallable leadsLocalReducerCallable = (LeadsLocalReducerCallable) result;
        LeadsLocalReducerCallable thisCallable = (LeadsLocalReducerCallable) this;
        leadsLocalReducerCallable.setPrefix(thisCallable.getPrefix());
        leadsLocalReducerCallable.setReducer(thisCallable.getReducer());
      } else if (result instanceof LeadsReducerCallable) {
        LeadsReducerCallable leadsReducerCallable = (LeadsReducerCallable) result;
        LeadsReducerCallable thisCallable = (LeadsReducerCallable) this;
        leadsReducerCallable.setPrefix(thisCallable.getPrefix());
        leadsReducerCallable.setReducer(thisCallable.getReducer());
      }
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return result;
  }

  public String getEnsembleHost() {
    return ensembleHost;
  }

  public void setEnsembleHost(String ensembleHost) {
    this.ensembleHost = ensembleHost;
  }

  public int getCallableIndex() {
    return callableIndex;
  }

  public void setCallableIndex(int index) {
    callableIndex = index;
  }

  public Queue<Map.Entry<K, V>> getInput() {
    return input;
  }

  public void setInput(Queue<Map.Entry<K, V>> input) {
    this.input = input;
  }

  public boolean isContinueRunning() {
    return continueRunning;
  }

  public void setContinueRunning(boolean continueRunning) {
    this.continueRunning = continueRunning;
  }

  @Override public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
    profilerLog = LoggerFactory.getLogger("###PROF###" + this.getClass().toString());

    EngineUtils.initialize();
    PrintUtilities.printAndLog(profilerLog,
        InfinispanClusterSingleton.getInstance().getManager().getMemberName().toString() + ": setupEnvironment");
    if (callableIndex == -1) {

      executeRunnables = new ArrayList<>(callableParallelism);

      callables = new ArrayList<>(callableParallelism);
      for (int i = 0; i < callableParallelism; i++) {
        PrintUtilities.printAndLog(profilerLog,
            InfinispanClusterSingleton.getInstance().getManager().getMemberName().toString() + ": setupEnvironment "
                + i);
        if (i == 0) {
          this.setCallableIndex(0);
          callables.add(this);
          ExecuteRunnable runnable = EngineUtils.getRunnable();
          runnable.setCallable(this);
          executeRunnables.add(runnable);
        } else {
          LeadsBaseCallable newCallable = this.copy();
          PrintUtilities.printAndLog(profilerLog,
              InfinispanClusterSingleton.getInstance().getManager().getMemberName().toString() + ": setupEnvironment "
                  + i + ".0");
          newCallable.setCallableIndex(i);
          newCallable.setEnvironment(cache, inputKeys);
          PrintUtilities.printAndLog(profilerLog,
              InfinispanClusterSingleton.getInstance().getManager().getMemberName().toString() + ": setupEnvironment "
                  + i + ".1");
          callables.add(newCallable);
          ExecuteRunnable runnable = EngineUtils.getRunnable();
          PrintUtilities.printAndLog(profilerLog,
              InfinispanClusterSingleton.getInstance().getManager().getMemberName().toString() + ": setupEnvironment "
                  + i + ".3");
          runnable.setCallable(newCallable);
          executeRunnables.add(runnable);
        }
      }
    }

    profCallable = new ProfileEvent("name", profilerLog);
    profCallable.setProfileLogger(profilerLog);
    if (profCallable != null) {
      profCallable.end("setEnv");
      profCallable.start("setEnvironment Callable ");
    } else
      profCallable = new ProfileEvent("setEnvironment Callable " + this.getClass().toString(), profilerLog);
    embeddedCacheManager = InfinispanClusterSingleton.getInstance().getManager().getCacheManager();
    imanager = new ClusterInfinispanManager(embeddedCacheManager);
    //    outputCache = (Cache) imanager.getPersisentCache(output);
    keys = inputKeys;
    this.inputCache = cache;
    ProfileEvent tmpprofCallable =
        new ProfileEvent("setEnvironment manager " + this.getClass().toString(), profilerLog);
    tmpprofCallable.start("Start LQPConfiguration");

    LQPConfiguration.initialize();
    tmpprofCallable.end();
    //    ensembleCacheUtilsSingle.initialize();
    //    ensembleCacheUtilsSingle = new EnsembleCacheUtilsSingle();
    if (ensembleHost != null && !ensembleHost.equals("")) {
      tmpprofCallable.start("Start EnsemlbeCacheManager");
      profilerLog.error("EnsembleHost EXIST " + ensembleHost);
      System.err.println("EnsembleHost EXIST " + ensembleHost);
      emanager = new EnsembleCacheManager(ensembleHost);
      //      ensembleCacheUtilsSingle.initialize(emanager);
      //      emanager.start();
      //      emanager = createRemoteCacheManager();
      //      ecache = emanager.getCache(output,new ArrayList<>(emanager.sites()),
      //          EnsembleCacheManager.Consistency.DIST);
    } else {
      profilerLog.error("EnsembleHost NULL");
      System.err.println("EnsembleHost NULL");
      tmpprofCallable.start("Start EnsemlbeCacheManager");
      emanager = new EnsembleCacheManager(LQPConfiguration.getConf().getString("node.ip") + ":11222");
      //      ensembleCacheUtilsSingle.initialize(emanager);
      //      emanager.start();
      //            emanager = createRemoteCacheManager();
    }
    emanager.start();

    tmpprofCallable.end();
    tmpprofCallable.start("Get cache ");
    ecache = emanager.getCache(output, new ArrayList<>(emanager.sites()), EnsembleCacheManager.Consistency.DIST);
    tmpprofCallable.end();
    outputCache = ecache;
    //outputCache =  emanager.getCache(output,new ArrayList<>(emanager.sites()),
    //          EnsembleCacheManager.Consistency.DIST);
    input = new LinkedList<>();
    initialize();
    profCallable.end("end_setEnv");

    start = System.currentTimeMillis();
    PrintUtilities.printAndLog(profilerLog,
        InfinispanClusterSingleton.getInstance().getManager().getMemberName().toString() + ": setupEnvironment "
            + ".end");
  }


  @Override public String call() throws Exception {
    profCallable.end("call");
    if (!isInitialized) {
      initialize();
    }
    profCallable.start("Call getComponent ()");
    final ClusteringDependentLogic cdl =
        inputCache.getAdvancedCache().getComponentRegistry().getComponent(ClusteringDependentLogic.class);
    String compressedCacheName = inputCache.getName() + ".compressed";
    if (inputCache.getCacheManager().cacheExists(compressedCacheName)) {
      Cache compressedCache = inputCache.getCacheManager().getCache(compressedCacheName);
      for (Object l : compressedCache.getListeners()) {
        if (l instanceof BatchPutListener) {
          BatchPutListener listener = (BatchPutListener) l;
          listener.waitForPendingPuts();
          break;
        }
      }
    }
    int count = 0;
    profCallable.end();
    //    ProfileEvent profExecute = new ProfileEvent("Buildinglucece" + this.getClass().toString(), profilerLog);

      profCallable.start("Iterate Over Local Data");
      System.out.println("Iterate Over Local Data");
  Object filter = new LocalDataFilter<K, V>(cdl);

      CloseableIterable iterable = inputCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
          .filterEntries((KeyValueFilter<? super K, ? super V>) filter);
      try {
        for (ExecuteRunnable runnable : executeRunnables) {
          EngineUtils.submit(runnable);
        }
        for (Object object : iterable) {
          //        profExecute.end();
          readCounter++;
          if (readCounter % 10000 == 0) {
            Thread.yield();
          }
          Map.Entry<K, V> entry = (Map.Entry<K, V>) object;

          //      V value = inputCache.get(key);
          //          K key = (K) entry.getKey();
          //          V value = (V) entry.getValue();

          if (entry.getValue() != null) {
            //          profExecute.start("ExOn" + (++count));
            //          ExecuteRunnable runable = EngineUtils.getRunnable();
            //          runable.setKeyValue(key, value,this);
            //          EngineUtils.submit(runable);
            //            executeOn((K) key, value);
            callables.get((readCounter % callableParallelism)).addToInput(entry);
            //          profExecute.end();
          }
          //         profExecute.start("ISPNIter");
        }
        iterable.close();
      } catch (Exception e) {
        iterable.close();
        if (e instanceof InterruptedException) {
          profilerLog.error(this.imanager.getCacheManager().getAddress().toString() + " was interrupted ");
        } else {
          profilerLog.error("Exception in LEADSBASEBACALLABE " + e.getClass().toString());
          PrintUtilities.logStackTrace(profilerLog, e.getStackTrace());
        }
      }

    //    profCallable.end();
    for (LeadsBaseCallable callable : callables) {
      callable.setContinueRunning(false);
    }
    System.err.println("----Engine wait ");
    EngineUtils.waitForAllExecute();
    for (LeadsBaseCallable callable : callables) {
      callable.finalizeCallable();
      System.err.println("--- callable finalized " + callable.getCallableIndex());
    }
    callables.clear();
    executeRunnables.clear();
    System.err.println("LAST LINE OF " + this.getClass().toString() + " " + embeddedCacheManager.getAddress().toString()
        + " ----------- END");
    profilerLog.error("LAST LINE OF " + this.getClass().toString() + " " + embeddedCacheManager.getAddress().toString()
        + " ----------- END");
    return embeddedCacheManager.getAddress().toString();
  }

  private synchronized void addToInput(Map.Entry<K, V> entry) {
    //    synchronized (input){
    input.add(entry);
    //    }
  }

  public synchronized Map.Entry poll() {
    Map.Entry result = null;
    //    synchronized (input){
    result = input.poll();
    //    }
    return result;
  }

  public void initialize() {
    if (isInitialized)
      return;
    isInitialized = true;
    if (configString != null || configString.length() > 0)
      conf = new JsonObject(configString);
  }

  @Override public void finalizeCallable() {
    try {
//      profCallable.start("finalizeBaseCallable");
      //      EngineUtils.waitForAllExecute();
      //      if(collector != null) {
      //        collector.finalizeCollector();
      //      }
      emanager.stop();
      ecache = null;
      //
      //      ecache.stop();
      //      outputCache.stop();
    } catch (Exception e) {
      System.err.println("LEADS Base callable " + e.getClass().toString() + " " + e.getMessage() + " cause ");
      profilerLog.error(("LEADS Base callable " + e.getClass().toString() + " " + e.getMessage() + " cause "));
      PrintUtilities.logStackTrace(profilerLog, e.getStackTrace());
    }
//    profCallable.end("finalizeBaseCallable");
    end = System.currentTimeMillis();
    profilerLog.error("LBDISTEXEC: " + this.getClass().toString() + " run for " + (end - start) + " ms");
  }

  //

  public void setConfigString(String configString) {
    this.configString = configString;
  }

  public void setOutput(String output) {
    this.output = output;
  }

  public void setCollector(LeadsCollector collector) {
    this.collector = collector;
  }

  public boolean isEmpty() {
    boolean result = false;
    synchronized (input) {
      result = input.isEmpty();
    }
    return result;
  }

  public void setLocalSite(String s) {
    collector.setLocalSite(s);
  }

  public LeadsCollector getCollector() {
    return collector;
  }



  public class qualinfo {
    String attributeName = "";
    String attributeType = "";
    FilterOpType opType;
    Object compValue = null;

    public qualinfo(String attributeName, String attributeType) {
      this.attributeName = attributeName;
      this.attributeType = attributeType;
      this.opType = opType;
      this.compValue = compValue;
    }

    public qualinfo(FilterOpType opType, qualinfo left, qualinfo right) throws Exception {
      this(left.attributeName, left.attributeType);
      complete(right);
      this.opType = opType;
    }

    public qualinfo(String attributeType, Object compValue) {
      this.attributeName = attributeName;
      this.attributeType = attributeType;
      this.opType = opType;
      this.compValue = compValue;
    }

    public qualinfo complete(qualinfo other) throws Exception {
      if (!this.attributeType.equals(other.attributeType)) {
        throw new Exception("Different Types " + this.attributeType + " " + other.attributeType);
      }


      if (attributeName.isEmpty()) {
        if (!other.attributeName.isEmpty()) {
          this.attributeName = other.attributeName;
        }
      } else {
        if (other.compValue != null) {
          this.compValue = other.compValue;
        }
      }

      return this;
    }
  }

}
