package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.common.infinispan.AcceptAllFilter;
import eu.leads.processor.common.infinispan.ClusterInfinispanManager;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.math.FilterOperatorTree;
import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;
import java.util.*;

/**
 * Created by vagvaz on 9/25/14.
 */
public class ScanCallable<K, V> implements

    DistributedCallable<K, V, String>, Serializable {
  transient protected Cache<K, V> inputCache;
  transient protected Cache outputCache;
  transient protected Cache pageRankCache;
  transient protected FilterOperatorTree tree;
  transient protected JsonObject inputSchema;
  transient protected JsonObject outputSchema;
  transient protected Map<String, String> outputMap;
  transient protected Map<String, List<JsonObject>> targetsMap;
  transient protected JsonObject conf;
  transient protected double totalSum;
  transient protected Cache approxSumCache;
  protected String configString;
  protected String output;
  protected String qualString;
  transient protected InfinispanManager manager;
  protected Logger log;

  public ScanCallable(String configString, String output) {
    this.configString = configString;
    this.output = output;
  }

  @Override public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
    inputCache = cache;

    manager = new ClusterInfinispanManager(cache.getCacheManager());
    log = LoggerFactory.getLogger("###### ScanCallable: " + manager.getMemberName().toString());
    log.info("--------------------    get output cache ------------------------");
    outputCache = (Cache) manager.getPersisentCache(output);
    log.info("--------------------    get pagerank cache ------------------------");
    pageRankCache = (Cache) manager.getPersisentCache("pagerankCache");
    log.info("--------------------    get approxSum cache ------------------------");
    approxSumCache = (Cache) manager.getPersisentCache("approx_sum_cache");
    totalSum = -1f;

    conf = new JsonObject(configString);
    if (conf.getObject("body").containsField("qual")) {
      tree = new FilterOperatorTree(conf.getObject("body").getObject("qual"));
    } else {
      tree = null;
    }

    outputSchema = conf.getObject("body").getObject("outputSchema");
    inputSchema = conf.getObject("body").getObject("inputSchema");
    targetsMap = new HashMap();
    outputMap = new HashMap<>();
    JsonArray targets = conf.getObject("body").getArray("targets");
    Iterator<Object> targetIterator = targets.iterator();
    while (targetIterator.hasNext()) {
      JsonObject target = (JsonObject) targetIterator.next();
      List<JsonObject> tars =
          targetsMap.get(target.getObject("expr").getObject("body").getObject("column").getString("name"));
      if (tars == null) {
        tars = new ArrayList<>();
      }
      tars.add(target);
      targetsMap.put(target.getObject("expr").getObject("body").getObject("column").getString("name"), tars);
    }
  }

  @Override public String call() throws Exception {
    log.info("--------------------   Iterate over values... ------------------------");

    final ClusteringDependentLogic cdl =
        inputCache.getAdvancedCache().getComponentRegistry().getComponent(ClusteringDependentLogic.class);

    //      for (Map.Entry<K, V> entry : inputCache.getAdvancedCache().getDataContainer().entrySet()) {
    for (Object key : inputCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet()) {
      if (!cdl.localNodeIsPrimaryOwner(key))
        continue;
      //         System.err.println(manager.getCacheManager().getAddress().toString() + " "+ entry.getKey() + "       " + entry.getValue());
      //VERSIONING
      //          String versionedKey = (String) key;
      //          String ikey = pruneVersion(versionedKey);
      //          Version latestVersion = versionedCache.getLatestVersion(ikey);
      //          if(latestVersion == null){
      //           continue;
      //           }
      //          Version currentVersion = getVersion(versionedKey);
      //       Object objectValue = versionedCache.get(ikey);
      //       String value = (String)objectValue;
      //END VERSIONING
      //NONVERSIONING
      String ikey = (String) key;
      //        String value = (String) inputCache.get(ikey);
      Tuple value = (Tuple) inputCache.get(ikey);
      //ENDNONVERSIONDING
      //         String value = (String) entry.getValue();

      //          String value = (String) entry.getValue();
      //          String value = (String)inputCache.get(key);
      //        Tuple tuple = new Tuple(value);
      Tuple tuple = value;
      namesToLowerCase(tuple);
      renameAllTupleAttributes(tuple);
      if (tree != null) {
        if (tree.accept(tuple)) {
          tuple = prepareOutput(tuple);
          //               log.info("--------------------    put into output with filter ------------------------");
          if (key != null && tuple != null)
            outputCache.put(key.toString(), tuple);
        }
      } else {
        tuple = prepareOutput(tuple);
        //            log.info("--------------------    put into output without tree ------------------------");
        if (key != null && tuple != null)
          outputCache.put(key.toString(), tuple);
      }

    }
    return inputCache.getCacheManager().getAddress().toString();
  }
 void namesToLowerCase(Tuple tuple) {
    Set<String> fieldNames = new HashSet<>(tuple.getFieldNames());
    for (String field : fieldNames) {
      tuple.renameAttribute(field, field.toLowerCase());
    }
  }

  private void renameAllTupleAttributes(Tuple tuple) {
    JsonArray fields = inputSchema.getArray("fields");
    Iterator<Object> iterator = fields.iterator();
    String columnName = null;
    while (iterator.hasNext()) {
      JsonObject tmp = (JsonObject) iterator.next();
      columnName = tmp.getString("name");
      int lastPeriod = columnName.lastIndexOf(".");
      String attributeName = columnName.substring(lastPeriod + 1);
      tuple.renameAttribute(attributeName, columnName);
    }

    handlePagerank(columnName.substring(0, columnName.lastIndexOf(".")), tuple);
  }

  protected Tuple prepareOutput(Tuple tuple) {
    if (outputSchema.toString().equals(inputSchema.toString())) {
      return tuple;
    }

    JsonObject result = new JsonObject();
    //WARNING
    //       System.err.println("out: " + tuple.asString());

    if (targetsMap.size() == 0) {
      //          System.err.println("s 0 ");
      return tuple;

    }
    //       System.err.println("normal");

    //END OF WANRING
    List<String> toRemoveFields = new ArrayList<String>();
    Map<String, List<String>> toRename = new HashMap<String, List<String>>();
    for (String field : tuple.getFieldNames()) {
      List<JsonObject> ob = targetsMap.get(field);
      if (ob == null)
        toRemoveFields.add(field);
      else {
        for (JsonObject obb : ob) {
          List<String> ren = toRename.get(field);
          if (ren == null) {
            ren = new ArrayList<>();
          }
          //               toRename.put(field, ob.getObject("column").getString("name"));
          ren.add(obb.getObject("column").getString("name"));
          toRename.put(field, ren);
        }
      }
    }
    tuple.removeAtrributes(toRemoveFields);
    tuple.renameAttributes(toRename);
    return tuple;
  }

  protected void handlePagerank(String substring, Tuple t) {
    if (conf.getObject("body").getObject("tableDesc").getString("tableName").equals("default.webpages")) {
      if (totalSum < 0) {
        computeTotalSum();
      }
      String url = t.getAttribute("default.webpages.url");

    }
  }

  private void computeTotalSum() {
    log.info("--------------------   Creating iterable over approx sum entries ------------------------");
    CloseableIterable<Map.Entry<String, Integer>> iterable =
        approxSumCache.getAdvancedCache().filterEntries(new AcceptAllFilter());
    log.info("--------------------    Iterating over approx sum entries cache ------------------------");
    for (Map.Entry<String, Integer> outerEntry : iterable) {
      totalSum += outerEntry.getValue();
    }
    if (totalSum > 0) {
      totalSum += 1;
    }
  }
}
