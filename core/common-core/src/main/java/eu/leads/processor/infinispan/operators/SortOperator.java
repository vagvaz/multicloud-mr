package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.EnsembleCacheUtils;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.core.TupleComparator;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.infinispan.LeadsMapper;
import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskBuilder;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.remoting.transport.Address;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 1:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class SortOperator extends BasicOperator {
  //    List<Column> columns;
  transient protected String[] sortColumns;
  transient protected Boolean[] asceding;
  transient protected String[] types;
  transient protected long rowcount = Long.MAX_VALUE;
  String prefix;
  List<String> addresses;

  public long getRowcount() {
    return rowcount;
  }


  private LeadsMapper<String, String, String, String> mapper;


  public SortOperator(Node com, InfinispanManager persistence, LogProxy logg, Action action) {
    super(com, persistence, logg, action);
    JsonArray sortKeys = conf.getObject("body").getArray("sortKeys");
    Iterator<Object> sortKeysIterator = sortKeys.iterator();
    sortColumns = new String[sortKeys.size()];
    asceding = new Boolean[sortKeys.size()];
    types = new String[sortKeys.size()];
    int counter = 0;
    while (sortKeysIterator.hasNext()) {
      JsonObject sortKey = (JsonObject) sortKeysIterator.next();
      sortColumns[counter] = sortKey.getObject("sortKey").getString("name");
      asceding[counter] = sortKey.getBoolean("ascending");
      types[counter] = sortKey.getObject("sortKey").getObject("dataType").getString("type");
      counter++;
    }
    if (conf.containsField("limit")) {
      rowcount = conf.getObject("limit").getObject("body").getLong("fetchFirstNum");
    } else {
      rowcount = -1;
    }

    if (isRemote) {
      prefix = action.getData().getString("prefix");
    } else {
      prefix = UUID.randomUUID().toString();
      action.getData().putString("prefix", prefix);
    }
    addresses = new ArrayList<>();
  }

  @Override public void init(JsonObject config) {
    //        super.init(config); //fix set correctly caches names
    //fix configuration
    init_statistics(this.getClass().getCanonicalName());
  }

  //   @Override
  public void ru2n() {
    long startTime = System.nanoTime();
    Cache inputCache = (Cache) this.manager.getPersisentCache(getInput());
    Cache beforeMerge = (Cache) this.manager.getPersisentCache(getOutput() + ".merge");
    Cache outputCache = (Cache) manager.getPersisentCache(getOutput());
    DistributedExecutorService des = new DefaultExecutorService(inputCache);
    //     String prefix = UUID.randomUUID().toString();
    SortCallableUpdated<String, Tuple> callable =
        new SortCallableUpdated(sortColumns, asceding, types, getOutput() + ".merge", prefix, rowcount);
    //      SortCallable callable = new SortCallable(sortColumns,asceding,types,getOutput()+".merge",UUID.randomUUID().toString());
    DistributedTaskBuilder builder = des.createDistributedTaskBuilder(callable);
    builder.timeout(1, TimeUnit.HOURS);
    DistributedTask task = builder.build();
    for (Address cacheNodes : inputCache.getAdvancedCache().getRpcManager().getMembers()) {
      String tmpCacheName = prefix + "." + cacheNodes.toString();
      manager.getPersisentCache(tmpCacheName);
    }
    manager.getPersisentCache(prefix + "." + manager.getMemberName().toString());
    List<Future<String>> res = des.submitEverywhere(task);
    List<String> addresses = new ArrayList<String>();
    try {
      if (res != null) {
        for (Future<?> result : res) {
          addresses.add((String) result.get());
        }
        System.out.println("sort callable  Execution is done on " + addresses.get(addresses.size() - 1));
      } else {
        System.out.println("sort callable Execution not done");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    //      Merge outputs

    //       Cache outputCache = (Cache) manager.getPersisentCache(getOutput());
    for (String cacheName : addresses) {
      manager.removePersistentCache(cacheName);
    }
    manager.removePersistentCache(beforeMerge.getName());
    //Single
    //      List<Tuple> tuples = new ArrayList<>();
    //      String prefix = getOutput()+":";
    //      try {
    //         CloseableIterable<Map.Entry<String, String>> iterable =
    //                 inputCache.getAdvancedCache().filterEntries(new AcceptAllFilter());
    //         for (Map.Entry<String, String> entry : iterable) {
    //
    //
    //            String valueString = (String) entry.getValue();
    //            if (valueString.equals(""))
    //               continue;
    //            tuples.add(new Tuple(valueString));
    //         }
    //      }catch (Exception e){
    //         e.printStackTrace();
    //      }
    //      Comparator<Tuple> comparator = new TupleComparator(sortColumns,asceding,types);
    //      Collections.sort(tuples, comparator);
    //      int counter = 0;
    //      for (Tuple t : tuples) {
    ////         while(outputCache.size() != counter+1) {
    //            outputCache.put(prefix + counter, t.asString());
    ////         }
    //         counter++;
    //      }
    //      tuples.clear();
    cleanup();
    //Store Values for statistics
    //      updateStatistics(inputCache.size(), manager.getPersisentCache(getOutput()).size(), System.nanoTime() - startTime);
    updateStatistics(inputCache, null, outputCache);
  }

  @Override public void cleanup() {
    super.cleanup();

  }

  @Override public void createCaches(boolean isRemote, boolean executeOnlyMap, boolean executeOnlyReduce) {
    //need to get the input Cache here because we need to get the nodes addresses in order to create the
    // necessary caches for the merge
    log.error("LGSORT: CREATE CACHES");
    inputCache = (Cache) manager.getPersisentCache(getInput());
    if (isRemote) {
      String coordinator = action.asJsonObject().getString("coordinator");
      //         String prefix = action.getData().getObject("data").getString("prefix");
      for (Address cacheNodes : inputCache.getAdvancedCache().getRpcManager().getMembers()) {
        String tmpCacheName = prefix + "." + currentCluster + "." + cacheNodes.toString();
        createCache(coordinator, tmpCacheName, "batchputListener");
      }
      log.error("LGSORT: " + getOutput() + ".addresses");
      Cache addressesCache = (Cache) this.manager.getPersisentCache(getOutput() + ".addresses");
      System.err.println("creating " + getOutput() + ".addresses to " + currentCluster);
      createCache(currentCluster, getOutput() + ".addresses", "batchputListener");
      System.err.println("creating " + getOutput() + ".addresses to " + coordinator);
      createCache(coordinator, getOutput() + ".addresses", "batchputListener");
      //         manager.getPersisentCache();
      createCache(coordinator, prefix + "." + currentCluster + "." + manager.getMemberName().toString(),
          "batchputListener");
    } else {
      //         if (executeOnlyMap) {
      //            if(pendingMMC.contains(currentCluster)) {
      for (Address cacheNodes : inputCache.getAdvancedCache().getRpcManager().getMembers()) {
        String tmpCacheName = prefix + "." + currentCluster + "." + cacheNodes.toString();
        //                  manager.getPersisentCache(tmpCacheName);
        createCache(currentCluster, tmpCacheName, "batchputListener");
      }

      createCache(currentCluster, prefix + "." + currentCluster + "." + manager.getMemberName().toString(),
          "batchputListener");
      //               manager.getPersisentCache(prefix+"."+currentCluster+"."+manager.getMemberName().toString());
      //            }


      Set<String> targetMC = getTargetMC();
      for (String mc : targetMC) {
        createCache(mc, getOutput(), "batchputlistener");
        System.err.println("in local creating ---" + getOutput() + ".addresses to " + mc);
        createCache(mc, getOutput() + ".addresses", "batchputListener");
      }
      System.err.println("in local creating " + getOutput() + ".addresses to " + currentCluster);
      log.error("LGSORT: COORD" + getOutput() + ".addresses");
      Cache addressesCache = (Cache) this.manager.getPersisentCache(getOutput() + ".addresses");
      createCache(currentCluster, getOutput() + ".addresses", "batchputListener");
    }
  }

  @Override public String getContinuousListenerClass() {
    return null;
  }

  @Override public void setMapperCallableEnsembleHost() {
    if (isRemote) {
      String ensembleHost =
          globalConfig.getObject("componentsAddrs").getArray(action.asJsonObject().getString("coordinator")).get(0)
              .toString();
      mapperCallable.setEnsembleHost(ensembleHost);
    } else {
      String ensembleHost = globalConfig.getObject("componentsAddrs").getArray(currentCluster).get(0).toString();
      mapperCallable.setEnsembleHost(ensembleHost);
    }
  }


  @Override public void setupMapCallable() {
    //      String prefix = UUID.randomUUID().toString();
    inputCache = (Cache) this.manager.getPersisentCache(getInput());
    mapperCallable = new SortCallableUpdated(sortColumns, asceding, types, getOutput() + ".merge", prefix, rowcount);
  }

  @Override public void setupReduceCallable() {

    Cache addressesCache = (Cache) this.manager.getPersisentCache(getOutput() + ".addresses");
    for (Object address : addressesCache.keySet()) {
      System.out.println("ICache " + address);
      addresses.add(address.toString());
    }
  }

  @Override public void executeReduce() {
    TupleComparator comparator = new TupleComparator(sortColumns, asceding, types);
    String ensembleHost = computeEnsembleHost();
    EnsembleCacheManager emanager = new EnsembleCacheManager(ensembleHost);
    emanager.start();
    SortMerger2 merger = new SortMerger2(addresses, getOutput(), comparator, manager, emanager, conf, getRowcount());
    merger.merge();
    try {
      EnsembleCacheUtils.waitForAllPuts();
    } catch (InterruptedException e) {
      e.printStackTrace();
      PrintUtilities.logStackTrace(log, e.getStackTrace());
    } catch (ExecutionException e) {
      e.printStackTrace();
      PrintUtilities.logStackTrace(log, e.getStackTrace());
    }
  }

  @Override public boolean isSingleStage() {
    return false;
  }


  public Boolean[] getAscending() {
    return this.asceding;
  }

  public void setAscending(Boolean[] ascending) {
    this.asceding = ascending;
  }


/*
    List<Boolean> ascending;

    public SortOperator(String name) {
        super(name, OperatorType.SORT);
    }

    public SortOperator(PlanNode node) {
        super(node, OperatorType.SORT);
    }

    @JsonCreator
    public SortOperator(@JsonProperty("name") String name, @JsonProperty("output") String output, @JsonProperty("columns") List<Column> orderByColumns, @JsonProperty("asceding") List<Boolean> ascendingOrder) {
        super(name, OperatorType.SORT);
        setOutput(output);
        this.columns = orderByColumns;
        this.ascending = ascendingOrder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(" ");
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getTable() != null)
                builder.append(columns.get(i).getWholeColumnName() + " " + (ascending.get(i) ? " ASC " : " DESC "));
            else
                builder.append(columns.get(i).getColumnName() + " " + (ascending.get(i) ? " ASC " : " DESC "));
        }
        return getType() + builder.toString();
    }
*/
}
