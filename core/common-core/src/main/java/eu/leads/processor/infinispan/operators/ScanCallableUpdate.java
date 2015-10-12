package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.infinispan.AcceptAllFilter;
import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.math.FilterOperatorTree;
import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;
import java.util.*;


/**
 * Created by vagvaz on 2/20/15.
 */
public class ScanCallableUpdate<K, V> extends LeadsSQLCallable<K, V> implements Serializable {


  transient protected Cache pageRankCache;
  transient protected double totalSum;
  transient protected Cache approxSumCache;
  transient boolean versioning;
  boolean onVersionedCache;
  transient protected long versionStart = -1, versionFinish = -1, range = -1;
  protected LeadsCollector collector;

  protected Logger log = LoggerFactory.getLogger(ScanCallableUpdate.class.toString());

  transient protected boolean renameTableInTree;
  transient private String toRename;
  transient private String tableName;
  transient Logger profilerLog;
  private ProfileEvent fullProcessing;


  public ScanCallableUpdate() {
    super();
  }

  public ScanCallableUpdate(String configString, String output, LeadsCollector collector) {
    super(configString, collector.getCacheName());
    this.collector = collector;
  }


  @Override public void initialize() {
    super.initialize();
    luceneKeys = null;

    profilerLog = LoggerFactory.getLogger("###PROF###" + this.getClass().toString());

    pageRankCache = (Cache) imanager.getPersisentCache("pagerankCache");
    log.info("--------------------    get approxSum cache ------------------------");
    approxSumCache = (Cache) imanager.getPersisentCache("approx_sum_cache");
    totalSum = -1f;


  }

  private boolean getHasNext() {
    boolean result = conf.containsField("next");
    return result;
  }

  private boolean checkIndex_usage() {
    // System.out.println("Check if fields are indexed");
    if (conf.getBoolean("useIndex")) {
      System.out.println("Check Callable Use indexes!!");
      indexCaches = new HashMap<>();
      String columnName;
      JsonArray fields = inputSchema.getArray("fields");
      Iterator<Object> iterator = fields.iterator();
      while (iterator.hasNext()) {
        JsonObject tmp = (JsonObject) iterator.next();
        columnName = tmp.getString("name");
        //System.out.print("Check if exists: " + "." + columnName + " ");
        if (imanager.getCacheManager().cacheExists(columnName)) {
          indexCaches.put(columnName, (Cache) imanager.getIndexedPersistentCache(columnName));
          System.out.println(columnName + " indexed!");
        }

      }
      return indexCaches.size() > 0;
    }
    return false;
  }

  /**
   * This method shoul read the Versions if any , from the configuration of the Scan operator and return true
   * if there are specific versions required, false otherwise
   *
   * @param conf the configuration of the operator
   * @return returns true if there is a query on specific versions false otherwise
   */
  private boolean getVersionPredicate(JsonObject conf) {
    return false;
  }

  @Override public void executeOn(K key, V ivalue) {

    //		ProfileEvent scanExecute = new ProfileEvent("ScanExecute", profilerLog);

    //         System.err.println(manager.getCacheManager().getAddress().toString() + " "+ entry.getKey() + "       " + entry.getValue());
    Tuple toRunValue = null;
    if (onVersionedCache) {
      String versionedKey = (String) key;

    } else {
      toRunValue = (Tuple) ivalue;
      if (attributeFunctions.size() > 0) {
        executeFunctions(toRunValue);
      }
    }
    Tuple tuple = toRunValue;//new Tuple(toRunValue);
    if (tree != null) {
      if (renameTableInTree) {
        tree.renameTableDatum(tableName, toRename);
      }

      //        profExecute.start("tree.accept");
      boolean accept = tree.accept(tuple);
      //        profExecute.end();
      if (accept) {
        //          profExecute.start("prepareOutput");

        tuple = prepareOutput(tuple);
        //          profExecute.end();
        //               log.info("--------------------    put into output with filter ------------------------");
        if (key != null && tuple != null) {
          //            profExecute.start("Scan_Put");
          collector.emit(key.toString(), tuple);
          //            EnsembleCacheUtils.putToCache(outputCache,key.toString(), tuple);
          //            profExecute.end();
        }
      }
    } else {
      //        profExecute.start("prepareOutput");
      tuple = prepareOutput(tuple);
      //        profExecute.end();
      //            log.info("--------------------    put into output without tree ------------------------");
      if (key != null && tuple != null) {
        //          profExecute.start("Scan_outputToCache");
        //          EnsembleCacheUtils.putToCache(outputCache,key, tuple);
        collector.emit(key.toString(), tuple);
        //          profExecute.end();
      }

    }
    //    scanExecute.end();
  }



  private boolean needsREnaming() {
    return !outputSchema.toString().equals(inputSchema.toString());
  }

  private String getRenamingTableFromSchema(JsonObject inputSchema) {
    if (inputSchema != null) {

      String fieldname = ((JsonObject) (inputSchema.getArray("fields").iterator().next())).getString("name");
      //fieldname database.table.collumncolumnName = tmp.getString("name");
      String result = fieldname.substring(fieldname.indexOf(".") + 1, fieldname.lastIndexOf("."));
      if (result != null && !result.equals(""))
        return result;
    }
    return null;
  }

  //TODO write checks
  private String getTableNameFromTuple(Tuple tuple) {
    if (tuple != null) {

      String fieldname = tuple.getFieldNames().iterator().next();
      //fieldname database.table.collumn
      String result = fieldname.substring(fieldname.indexOf(".") + 1, fieldname.lastIndexOf("."));
      if (result != null && !result.equals(""))
        return result;
    }

    return null;
  }


  private void computeTotalSum() {
    log.info("--------------------   Creating iterable over approx sum entries ------------------------");
    CloseableIterable<Map.Entry<String, Integer>> iterable =
        approxSumCache.getAdvancedCache().filterEntries(new AcceptAllFilter());
    log.info("--------------------    Iterating over approx sum entries cache ------------------------");
    for (Map.Entry<String, Integer> outerEntry : iterable) {
      totalSum += outerEntry.getValue();
    }
    iterable.close();

    if (totalSum > 0) {
      totalSum += 1;
    }
  }

  @Override public void finalizeCallable() {
    fullProcessing.end();
    collector.finalizeCollector();
    super.finalizeCallable();
  }

}
