package eu.leads.processor.infinispan.operators;

import com.google.common.hash.BloomFilter;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.core.plan.LeadsNodeType;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.math.FilterOperatorNode;
import eu.leads.processor.math.FilterOperatorTree;
import eu.leads.processor.math.MathUtils;
import org.infinispan.Cache;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by vagvaz on 9/22/14.
 */
public class ScanOperator extends BasicOperator {

  boolean joinOperator;
  boolean groupByOperator;
  boolean sortOperator;

  public ScanOperator(Node com, InfinispanManager persistence, LogProxy log, Action action) {
    super(com, persistence, log, action);
  }

  //  public FilterOperator(PlanNode node) {
  //      super(node, OperatorType.FILTER);
  //  }


  @Override public void init(JsonObject config) {
    inputCache = (Cache) manager.getPersisentCache(getInput());
    if (conf.containsField("next")) {
      if (conf.getString("next.type").equals(LeadsNodeType.GROUP_BY.toString())) {
        groupByOperator = true;//new GroupByOperator(this.com,this.manager,this.log,this.action);
        //            PlanNode node = new PlanNode(conf.getObject("next").asObject());
        //            groupByOperator.setIntermediateCacheName(node.getNodeId()+".intermediate");
        //            groupByOperator.init(conf.getObject("next").getObject("configuration"));
      } else if (conf.getString("next.type").equals(LeadsNodeType.JOIN.toString())) {
        //        if(conf.getObject("next").containsField("buildBloom")){
        //          conf.getObject("next").getObject("configuration").putObject("buildBloom",conf.getObject("next").getObject("buildBloom"));
        //        }
        joinOperator = true;// new JoinOperator(this.com,this.manager,this.log,this.action);
        //            PlanNode node = new PlanNode(conf.getObject("next").asObject());
        //            joinOperator.setIntermediateCacheName(node.getNodeId()+".intermediate");
        //            joinOperator.init(conf.getObject("next").getObject("configuration"));
      } else if (conf.getString("next.type").equals(LeadsNodeType.SORT.toString())) {
        sortOperator = true;//new SortOperator(this.com,this.manager,this.log,this.action);
        //            sortOperator.init(conf.getObject("next").getObject("configuration"));
        System.err.println("SORT SCAN NOT IMPLEMENTED YET");
      } else {
        System.err.println(conf.getString("next.type") + " SCAN NOT IMPLEMENTED YET");
      }
    }
    ProfileEvent scanExecute = new ProfileEvent("OperatorcheckIndex_usage", profilerLog);
    if (checkIndex_usage())
      conf.putBoolean("useIndex", true);
    else
      conf.putBoolean("useIndex", false);
    scanExecute.end();
  }


  @Override public void cleanup() {
    if (conf.containsField("next")) {
      if (conf.getObject("next").getObject("configuration").containsField("buildBloomFilter")) {
        System.err.println("Building centralized BF");
        JsonObject bloomFilter = conf.getObject("next").getObject("configuration").getObject("buildBloomFilter");
        Cache<String, BloomFilter> bloomCache = (Cache) manager.getPersisentCache(bloomFilter.getString("bloomCache"));
        BloomFilter centralized = null;
        for (String key : bloomCache.keySet()) {
          System.err.println("NODE BF: " + key);
          if (centralized == null) {
            centralized = bloomCache.get(key);
          } else {
            centralized.putAll(bloomCache.get(key));
          }
          bloomCache.remove(key);
        }

        Set<String> set = getMicroCloudsFromOpTarget();
        for (String mc : set) {
          EnsembleCacheManager tmpmanager =
              new EnsembleCacheManager(globalConfig.getObject("componentsAddrs").getArray(mc).get(0).toString());
          EnsembleCache ensembleBloomCache = tmpmanager.getCache(bloomFilter.getString("bloomCache"));
          ensembleBloomCache.put(LQPConfiguration.getInstance().getMicroClusterName(), centralized);
        }
      } else if (conf.getObject("next").getObject("configuration").containsField("useBloomFilter")) {
        JsonObject bloomFilter = conf.getObject("next").getObject("configuration").getObject("useBloomFilter");
        manager.removePersistentCache(bloomFilter.getString("bloomCache"));

      }

    }
    System.err.println("CLEANING UP ");
    super.cleanup();
  }

  @Override public void createCaches(boolean isRemote, boolean executeOnlyMap, boolean executeOnlyReduce) {
    Set<String> targetMC = getTargetMC();
    for (String mc : targetMC) {
      if (!conf.containsField("next")) {
        createCache(mc, getOutput(), "batchputListener");
      } else {
        createCache(mc, getOutput() + ".data", "localIndexListener:batchputListener");
        if (conf.containsField("next")) {
          if (conf.getObject("next").containsField("buildBloomFilter")) {
            JsonObject bloomFilter = conf.getObject("next").getObject("configuration").getObject("buildBloomFilter");
            createCache(mc, bloomFilter.getString("bloomCache"));

          }
        }
      }

    }
  }

  @Override public String getContinuousListenerClass() {
    return null;
  }

  @Override public void setupMapCallable() {
    inputCache = (Cache) manager.getPersisentCache(getInput());
    LeadsCollector collector = new LeadsCollector<>(0, getOutput());
    mapperCallable = new ScanCallableUpdate<>(conf.toString(), getOutput(), collector);
  }

  @Override public String getOutput() {
    String result = super.getOutput();
    if (groupByOperator) {
      result = conf.getObject("next").getString("id") + ".intermediate";
    }
    if (joinOperator) {
      result = conf.getObject("next").getString("id") + ".intermediate";
    }
    if (sortOperator) {

    }
    return result;
  }

  @Override public void setupReduceCallable() {

  }

  @Override public boolean isSingleStage() {
    return true;
  }


  private boolean checkIndex_usage() {
    if (conf.getObject("body").containsField("qual")) {
      System.out.println("Scan Check if fields are indexed.");
      JsonObject inputSchema;
      inputSchema = conf.getObject("body").getObject("inputSchema");
      JsonArray fields = inputSchema.getArray("fields");
      System.out.println("Check if inputSchema fields: " + fields.toArray().toString() + " are indexed.");

      Iterator<Object> iterator = fields.iterator();
      String columnName = null;
      HashMap indexCaches = new HashMap<>();
      HashMap sketches = new HashMap<>();
      while (iterator.hasNext()) {
        JsonObject tmp = (JsonObject) iterator.next();
        columnName = tmp.getString("name");
        //System.out.print("Check if exists: " +  columnName + " ");
        if (manager.getCacheManager().cacheExists(columnName)) {
          indexCaches.put(columnName, (Cache) manager.getIndexedPersistentCache(columnName));
          System.out.print(columnName + " exists! ");
        }

        if (manager.getCacheManager().cacheExists(columnName + ".sketch")) {

          System.out.println(" exists!");
        }
      }

      if (indexCaches.size() == 0) {
        System.out.println("Nothing Indexed");
        return false;
      } else {
        System.out.print("At least some fields are Indexed: ");
        for (Object s : indexCaches.keySet())
          System.out.println((String) s);
      }
      long start = System.currentTimeMillis();

      FilterOperatorTree tree = new FilterOperatorTree(conf.getObject("body").getObject("qual"));
      Object selectvt = null;
      System.out.println(
          "  selectvt CMS " + selectvt + "  computation time: " + (System.currentTimeMillis() - start) / 1000.0);
      long inputSize;
      if (selectvt != null) {
        start = System.currentTimeMillis();
        System.out.println("Get size of table " + columnName.substring(0, columnName.lastIndexOf(".")));
        Cache<String, Long> sizeC = (Cache) manager.getPersisentCache("TablesSize");
        if (sizeC.containsKey(columnName.substring(0, columnName.lastIndexOf("."))))
          inputSize = sizeC.get(columnName.substring(0, columnName.lastIndexOf(".")));
        else {
          System.out.print("Size not found, Slow Get size() ");
          inputSize = inputCache.size();
          System.out.println("... Caching size value.");
          sizeC.put(columnName.substring(0, columnName.lastIndexOf(".")), inputSize);
        }
        System.out.println(" Found size: " + inputSize);

        double selectivity = (double) selectvt / (double) inputSize;
        System.out.println("Scan  Selectivity: " + selectivity);
        System.out.println("  Selectivity, inputSize " + inputSize + "  computation time: "
            + (System.currentTimeMillis() - start) / 1000.0);

        if (selectivity < 0.5) {
          System.out.println("Scan Use indexes!! ");
          return indexCaches.size() > 0;
        }
      } else
        System.out.println("No Selectivity!!");

    } else
      System.out.println("No Qual!!");

    System.out.println("Don't Use indexes!! ");
    return false;
  }

}
