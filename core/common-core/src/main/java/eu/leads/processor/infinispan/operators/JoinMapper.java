package eu.leads.processor.infinispan.operators;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsMapper;
import eu.leads.processor.math.FilterOperatorTree;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by vagvaz on 11/21/14.
 */
public class JoinMapper extends LeadsMapper<String, Tuple, String, Tuple> {

  //   private String configString;
  transient private String tableName;
  transient Map<String, List<String>> joinColumns;
  transient ProfileEvent profileEvent;
  transient Logger profilerLog;
  transient ProfileEvent mapEvent;
  private transient FilterOperatorTree tree;
  private transient Map<String, List<String>> tableCols;
  private transient BloomFilter bloomFilter;
  private transient boolean buildBloom;
  private transient boolean filterBloom;
  private transient String bloomCacheName;
  private transient Cache bloomCache;
  private int filtered = 0;

  //  private int counter = 0;
  public JoinMapper() {
  }

  ;

  public JoinMapper(String s) {
    super(s);
  }

  @Override public void map(String key, Tuple t, Collector<String, Tuple> collector) {

    //    if (!isInitialized)
    //      initialize();
    if (tableName == null) {
      tableName = resolveTableName(t, tableCols);
    }
    //    mapEvent.start("JoinMapMapExecute");
    StringBuilder builder = new StringBuilder();
    //        String tupleId = key.substring(key.indexOf(":"));
    //Tuple t = new Tuple(value);
    //        Tuple t = new Tuple(value);
    //progress();
    //    profileEvent.start("JoinMapReadJoinAttributes");
    for (String c : joinColumns.get(tableName)) {
      builder.append(t.getGenericAttribute(c).toString() + ",");
    }
    //    profileEvent.end();
    //    profileEvent.start("JoinMapPrepareOutput");
    String outkey = builder.toString();
    outkey.substring(0, outkey.length() - 1);
    //           collector.emit(outkey, t.asString());
    t.setAttribute("__table", tableName);
    t.setAttribute("__tupleKey", key);
    //    profileEvent.end();
    //    profileEvent.start("JoinMapEMitIntermediateResult");
    //    counter++;
    if (buildBloom) {
      bloomFilter.put(outkey);
    }
    if (filterBloom) {
      if (!bloomFilter.mightContain(outkey)) {
        filtered++;
        return;
      }
    }
    collector.emit(outkey, t);
    //    profileEvent.end();
    //    mapEvent.end();
  }

  private String resolveTableName(Tuple t, Map<String, List<String>> tableCols) {
    String result = null;
    for (Map.Entry<String, List<String>> entry : tableCols.entrySet()) {
      String column = entry.getValue().get(0);
      if (t.hasField(column)) {
        tableName = entry.getKey();
        result = entry.getKey();
        break;
      }
    }

    System.err.println("Table Name is resolved " + tableName);
    return result;
  }



  @Override public void initialize() {
    isInitialized = true;
    profilerLog = LoggerFactory.getLogger(JoinMapper.class);
    profileEvent = new ProfileEvent("JoinMapInitialize", profilerLog);
    mapEvent = new ProfileEvent("JoinMapExecute ", profilerLog);

    //       System.err.println("-------------Initialize");
    super.initialize();
    JsonElement qual = conf.getObject("body").getElement("joinQual");

    tree = new FilterOperatorTree(qual);
    tableCols = tree.getJoinColumns();
    //Infer columns
    JsonObject ob = new JsonObject();
    for (Map.Entry<String, List<String>> entry : tableCols.entrySet()) {
      JsonArray array = new JsonArray();
      for (String col : entry.getValue()) {
        array.add(col);
      }
      ob.putArray(entry.getKey(), array);
    }
    conf.putObject("joinColumns", ob);


    JsonObject jCols = conf.getObject("joinColumns");
    System.err.println(jCols.encodePrettily());
    Set<String> tables = new HashSet<>();
    tables.addAll(jCols.getFieldNames());
    joinColumns = new HashMap<>();
    for (String table : tables) {

      joinColumns.put(table, new ArrayList<String>());

      Iterator<Object> columnsIterator = jCols.getArray(table).iterator();
      while (columnsIterator.hasNext()) {
        String current = (String) columnsIterator.next();
        joinColumns.get(table).add(current);
      }
      System.err.println(table);
      PrintUtilities.printList(joinColumns.get(table));
    }

    tableName = conf.getString("inputCache");
    System.err.println("tablename " + tableName);
    if (conf.containsField("buildBloomFilter")) {
      System.err.println("building bloom filter");
      bloomCacheName = conf.getObject("buildBloomFilter").getString("bloomCache");
      System.err.println("bloom cache is " + bloomCacheName);
      buildBloom = true;
      bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1000000);
    } else {
      buildBloom = false;
      System.err.println("Do NOT BUILD BF");
    }
    if (conf.containsField("useBloomFilter")) {
      filterBloom = true;
      System.err.println("USING BLOOM FILTER...");
      bloomCacheName = conf.getObject("useBloomFilter").getString("bloomCache");
      bloomCache = (Cache) InfinispanClusterSingleton.getInstance().getManager().getPersisentCache(bloomCacheName);
      for (Object o : bloomCache.keySet()) {
        String mc = (String) o;
        System.err.println("MC BF: " + mc);
        if (bloomFilter == null) {
          bloomFilter = (BloomFilter) bloomCache.get(mc);
        } else {
          bloomFilter.putAll((BloomFilter) bloomCache.get(mc));
        }
      }
    } else {
      System.err.println("DO NOT FILTER BF");
      filterBloom = false;
    }
    profileEvent.end();
  }

  @Override protected void finalizeTask() {
    if (buildBloom) {
      Cache cache = (Cache) InfinispanClusterSingleton.getInstance().getManager().getPersisentCache(bloomCacheName);
      cache.put(InfinispanClusterSingleton.getInstance().getManager().getCacheManager().getAddress().toString(),
          bloomFilter);
    }
    if (filterBloom) {
      System.err.println("Filtered tuples: " + filtered);
      profilerLog.error("Filtered tuples: " + filtered);
    } else {
      System.err.println("NO BLOOM FILTER");
      profilerLog.error("NO BLOOM FILTER" + filtered);
    }
    super.finalizeTask();
    //    System.err.println("\n\n\nC: " + counter);
  }
}
