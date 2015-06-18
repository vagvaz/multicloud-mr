package eu.leads.processor.infinispan.operators;

import com.hazelcast.logging.LoggerFactory;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.QualFilter;
import eu.leads.processor.math.FilterOperatorTree;
import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.slf4j.Logger;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 2/20/15.
 */
public class JoinCallableUpdated<K,V> extends LeadsSQLCallable<K,V> {
  transient  protected JsonObject joinQual;
  transient protected String tableName;
  transient protected String otherTableName;
  transient protected Cache outerCache;
  transient protected FilterOperatorTree tree;
  transient protected Map<String,List<Tuple>> groups;
  protected String configString;
  protected String output;
  protected boolean left;
  protected String innerColumn;
  protected String outerColumn;
  protected final String outerCacheName;
  protected Logger log = org.slf4j.LoggerFactory.getLogger(JoinCallableUpdated.class);
  public JoinCallableUpdated(String configString, String output,String outerCacheName,boolean left) {
    super(configString, output);
    this.outerCacheName = outerCacheName;
    this.left = left;
  }

  @Override public void initialize() {
    super.initialize();
    outerCache = (Cache) imanager.getPersisentCache(outerCacheName);
    JsonObject object = conf.getObject("body").getObject("joinQual");
    joinQual = new JsonObject();
    joinQual.mergeIn(object);
    tree = new FilterOperatorTree(object);
  }

  @Override public void executeOn(K ikey, V ivalue) {
    CloseableIterable<Map.Entry<String, Tuple>> iterable = null;
    try{
      log.error("read tuple");
//      Tuple current = new Tuple((String)inputCache.get(ikey));
      Tuple current = (Tuple)ivalue;
      //          String columnValue = current.getGenericAttribute(innerColumn).toString();
      String key = (String) ikey;
      String currentKey = key.substring(key.indexOf(":") + 1);
      log.error("update tree with current");
      tree.getRoot().updateWith(current);
      //          CloseableIterable<Map.Entry<String, String>> iterable =
      //            outerCache.getAdvancedCache().filterEntries(new AttributeFilter(outerColumn,
      //
      //                                                                             columnValue));
      Map<String, List<Tuple>> buffer = new HashMap<String, List<Tuple>>();
      int size = 0;
      ArrayList<String> ignoreColumns = new ArrayList<>();
      //        ignoreColumns.add(innerColumn);
      //        ignoreColumns.add(outerColumn);
      String prefix = output + ":";
    log.error("start iteratorble");
      iterable =
        outerCache.getAdvancedCache().filterEntries(new QualFilter(tree.getJson().toString()));
      for (Map.Entry<String, Tuple> outerEntry : iterable) {
//        Tuple outerTuple = new Tuple(outerEntry.getValue());
        Tuple outerTuple = outerEntry.getValue();
        log.error("join tuples");
        Tuple resultTuple = new Tuple(current, outerTuple, ignoreColumns);
        String outerKey = outerEntry.getKey().substring(outerEntry.getKey().indexOf(":") + 1);
        String combinedKey = prefix + outerKey + "-" + currentKey;
        log.error("prepare output");
        resultTuple = prepareOutput(resultTuple);
        log.error("output tpule");
        outputCache.put(combinedKey, resultTuple);
      }
      iterable.close();

    }catch (Exception e) {
      if(iterable != null)
        iterable.close();
      System.err.println("Iterating over " + outerCacheName
                           + " for batch resulted in Exception " + e.getClass().toString()  + "\n"
                           + e.getMessage() + "\n from  " + outerCacheName);
    }
  }

}

