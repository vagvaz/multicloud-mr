package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsMapper;
import org.infinispan.distexec.mapreduce.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

import java.util.*;

/**
 * Created by vagvaz on 11/21/14.
 */
public class JoinMapper extends LeadsMapper<String,Tuple,String,Tuple> {

  //   private String configString;
  transient private String tableName;
  transient Map<String, List<String>> joinColumns;
  transient ProfileEvent profileEvent;
  transient Logger profilerLog;
  transient ProfileEvent mapEvent;
  public JoinMapper(String s) {
    super(s);
  }

  @Override
  public void map(String key, Tuple t, Collector<String, Tuple> collector) {

    if (!isInitialized)
      initialize();
    mapEvent.start("JoinMap MapExecute");
    StringBuilder builder = new StringBuilder();
    //        String tupleId = key.substring(key.indexOf(":"));
    //Tuple t = new Tuple(value);
    //        Tuple t = new Tuple(value);
    //progress();
    profileEvent.start("JoinMapReadJoinAttributes");
    for (String c : joinColumns.get(tableName)) {
      builder.append(t.getGenericAttribute(c).toString() + ",");
    }
    profileEvent.end();
    profileEvent.start("JoinMapPrepareOutput");
    String outkey = builder.toString();
    outkey.substring(0, outkey.length() - 1);
    //           collector.emit(outkey, t.asString());
    t.setAttribute("__table", tableName);
    t.setAttribute("__tupleKey",key);
    profileEvent.end();
    profileEvent.start("JoinMapEMitIntermediateResult");
    collector.emit(outkey, t);
    profileEvent.end();
    mapEvent.end();
  }



  @Override
  public void initialize() {
    isInitialized = true;
    profilerLog = LoggerFactory.getLogger(JoinMapper.class);
    profileEvent = new ProfileEvent("JoinMapInitialize",profilerLog);
    mapEvent = new ProfileEvent("JoinMapExecute ",profilerLog);

    //       System.err.println("-------------Initialize");
    super.initialize();
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
    profileEvent.end();
  }
}
