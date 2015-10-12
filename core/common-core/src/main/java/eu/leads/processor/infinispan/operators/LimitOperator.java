package eu.leads.processor.infinispan.operators;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import eu.leads.processor.common.infinispan.AcceptAllFilter;
import eu.leads.processor.common.infinispan.EnsembleCacheUtils;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

//import eu.leads.processor.plan.ExecutionPlanNode;
//import eu.leads.processor.sql.PlanNode;
//import net.sf.jsqlparser.statement.select.Limit;


/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 1:00 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect public class LimitOperator extends BasicOperator {
  boolean sorted = false;
  //    Cache inputMap=null;
  BasicCache data = null;
  public String prefix;
  public String inputPrefix;
  public long rowCount;
  private EnsembleCacheManager emanager;

  public LimitOperator(Action action) {
    super(action);
  }

  public LimitOperator(Node com, InfinispanManager persistence, LogProxy log, Action action) {

    super(com, persistence, log, action);
    rowCount = conf.getObject("body").getLong("fetchFirstNum");
    sorted = conf.getBoolean("isSorted");
    prefix = getOutput() + ":";
    inputCache = (Cache) persistence.getPersisentCache(getInput());
    emanager = new EnsembleCacheManager(computeEnsembleHost());
    emanager.start();
    data = emanager.getCache(getOutput(), new ArrayList<>(emanager.sites()), EnsembleCacheManager.Consistency.DIST);
    //      data = persistence.getPersisentCache(getOutput());
    inputPrefix = inputCache.getName() + ":";
  }

  @Override public void init(JsonObject config) {
    super.init(conf);
    ///How to initialize what ?
    rowCount = conf.getObject("body").getLong("fetchFirstNum");
    init_statistics(this.getClass().getCanonicalName());
    EnsembleCacheUtils.initialize();

  }

  @Override public void executeMap() {

    subscribeToMapActions(pendingMMC);
    if (!isRemote) {
      for (String mc : pendingMMC) {
        if (!mc.equals(currentCluster)) {
          sendRemoteRequest(mc, true);
        }
      }
    }

    if (pendingMMC.contains(currentCluster)) {
      int counter = 0;
      if (sorted) {
        //            int sz = inputMap.size();
        //          CloseableIterable<Map.Entry<String, String>> iterable =
        //            inputMap.getAdvancedCache().filterEntries(new AcceptAllFilter());
        //          for (Map.Entry<String, String> entry : iterable) {
        //              System.err.println("e: " + entry.getKey().toString() + " ---> " + entry.getValue().toString());
        //            }
        for (counter = 0; counter < rowCount; counter++) {
          //                String tupleValue = (String) inputMap.get(inputPrefix + counter);
          Tuple tupleValue = (Tuple) inputCache.get(inputPrefix + counter);
          if (tupleValue == null)
            break;
          //                System.err.println("Read " + inputPrefix + counter + " --> " +tupleValue);
          Tuple t = tupleValue;
          handlePagerank(t);
          //                System.err.println(prefix+counter);
          EnsembleCacheUtils.putToCacheDirect(data, prefix + Integer.toString(counter), t);
          //                data.put(prefix + Integer.toString(counter), t.asString());
        }
      } else {
        CloseableIterable<Map.Entry<String, Tuple>> iterable =
            inputCache.getAdvancedCache().filterEntries(new AcceptAllFilter(rowCount));
        for (Map.Entry<String, Tuple> entry : iterable) {
          if (counter >= rowCount)
            break;
          String tupleId = entry.getKey().substring(entry.getKey().indexOf(":") + 1);
          Tuple t = entry.getValue();
          handlePagerank(t);
          EnsembleCacheUtils.putToCacheDirect(data, prefix + tupleId, t);
          counter++;
        }
        iterable.close();

      }
      try {
        EnsembleCacheUtils.waitForAllPuts();
      } catch (InterruptedException e) {
        e.printStackTrace();
        PrintUtilities.logStackTrace(log, e.getStackTrace());
      } catch (ExecutionException e) {
        e.printStackTrace();
        PrintUtilities.logStackTrace(log, e.getStackTrace());
      }
      replyForSuccessfulExecution(action);
    }

    synchronized (mmcMutex) {
      while (pendingMMC.size() > 0) {
        System.out.println("Sleeping to executing " + this.getClass().toString() + " pending clusters ");
        PrintUtilities.printList(Arrays.asList(pendingMMC));
        try {
          mmcMutex.wait(120000);
        } catch (InterruptedException e) {
          log.error("Interrupted " + e.getMessage());
          break;
        }
      }
    }
    for (Map.Entry<String, String> entry : mcResults.entrySet()) {
      System.out.println("Execution on " + entry.getKey() + " was " + entry.getValue());
      log.error("Execution on " + entry.getKey() + " was " + entry.getValue());
      if (entry.getValue().equals("FAIL"))
        failed = true;
    }

  }

  @Override public void run() {
    //      createCaches(isRemote,executeOnlyMap,executeOnlyReduce);
    long startTime = System.nanoTime();
    findPendingMMCFromGlobal();
    findPendingRMCFromGlobal();
    createCaches(isRemote, executeOnlyMap, executeOnlyReduce);
    if (executeOnlyMap) {
      //        setupMapCallable();
      executeMap();
    }
    if (!failed) {

    } else {
      failCleanup();
    }

    //Store Values for statistics
    //        updateStatistics(inputMap.size(), data.size(), System.nanoTime() - startTime);
    updateStatistics(inputCache, null, data);
    cleanup();
  }

  private void handlePagerank(Tuple t) {
    if (t.hasField("pagerank")) {
      if (!t.hasField("url"))
        return;
      String pagerankStr = t.getAttribute("pagerank");
      //            Double d = Double.parseDouble(pagerankStr);
      //            if (d < 0.0) {

      //                try {
      ////                    d = LeadsPrGraph.getPageDistr(t.getAttribute("url"));
      //                    d = (double) LeadsPrGraph.getPageVisitCount(t.getAttribute("url"));
      //                    System.out.println("vs cnt " + LeadsPrGraph.getTotalVisitCount());
      //                } catch (IOException e) {
      //                    e.printStackTrace();
      //                }
      //                t.setAttribute("pagerank", d.toString());
      //            }
    }
  }


  @Override public void createCaches(boolean isRemote, boolean executeOnlyMap, boolean executeOnlyReduce) {
    Set<String> targetMC = getTargetMC();
    for (String mc : targetMC) {
      createCache(mc, getOutput(), "batchputListener");
    }
  }

  @Override public String getContinuousListenerClass() {
    return null;
  }

  @Override public void setupMapCallable() {

  }

  @Override public void setupReduceCallable() {

  }

  @Override public boolean isSingleStage() {
    return true;
  }
}/*ExecutionPlanNode {
    private Limit limit;

    public Limit getLimit() {
        return limit;
    }

    public void setLimit(Limit limit) {
        this.limit = limit;
    }

    public LimitOperator(String name) {
        super(name, OperatorType.LIMIT);
    }


    private LimitOperator(PlanNode node) {
        super(node, OperatorType.LIMIT);
    }

    @JsonCreator
    public LimitOperator(@JsonProperty("name") String name, @JsonProperty("output") String output, @JsonProperty("limit") Limit limit) {
        super(name, OperatorType.LIMIT);
        setOutput(output);
        this.limit = limit;
    }

    @Override
    public String toString() {
        return getType() + " " + limit.getRowCount();
    }
}*/
