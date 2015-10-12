package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsMapper;
import org.infinispan.distexec.mapreduce.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//import org.infinispan.distexec.mapreduce.Collector;


/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/3/13
 * Time: 4:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class GroupByMapper extends LeadsMapper<String, Tuple, String, Tuple> {

  transient List<String> columns;
  transient Logger log;
  //    transient private ProfileEvent groupEvent;
  //    transient private ProfileEvent mapEvent;
  transient private Tuple emptyTuple;
  transient private StringBuilder builder;
  transient private Collector<String, Tuple> lc = null;
  transient private Tuple countTuple = null;
  int groupcounter = 0;

  public GroupByMapper(JsonObject configuration) {
    super(configuration);
    columns = new ArrayList<String>();
  }

  public GroupByMapper() {
  }

  public GroupByMapper(String configString) {
    super(configString);
  }


  @Override public void map(String key, Tuple value, Collector<String, Tuple> collector) {
    //      System.out.println("Called for " + key + "     " + value);
    //       mapEvent.start("mapEvent");
    if (lc == null) {
      lc = collector;
    }
    builder.delete(0, builder.length());
    //        String tupleId = key.substring(key.indexOf(":"));
    Tuple t = (value);
    //        Tuple t = new Tuple(value);
    //progress();
    for (String c : columns) {
      builder.append(t.getGenericAttribute(c).toString() + ",");
    }
    if (columns.size() != 0) {
      //           System.out.println("+++++++++++ " + t.asString() + " normal at " + builder.toString());
      String outkey = builder.toString();
      outkey.substring(0, outkey.length() - 1);
      //           collector.emit(outkey, t.asString());
      collector.emit(outkey, t);
    } else {
      //           System.out.println("**************" + t.asString() + " emit");
      //           collector.emit("***" ,  emptyTuple);
      groupcounter++;
      //           collector.emit("***" ,  t.asString());
    }
    //       mapEvent.end();
  }

  @Override public void initialize() {
    super.initialize();
    emptyTuple = new Tuple();
    isInitialized = true;
    log = LoggerFactory.getLogger(GroupByMapper.class);
    super.initialize();
    JsonArray columnArray = conf.getObject("body").getArray("groupingKeys");
    Iterator<Object> columnsIterator = columnArray.iterator();
    columns = new ArrayList<String>(columnArray.size());
    while (columnsIterator.hasNext()) {
      JsonObject current = (JsonObject) columnsIterator.next();
      columns.add(current.getString("name"));
    }
    builder = new StringBuilder();
    //        groupEvent = new ProfileEvent("groupbymap",log);
    //        mapEvent = new ProfileEvent("groupbymap",log);
  }

  @Override protected void finalizeTask() {
    //        groupEvent.end();
    if (columns.size() == 0) {
      if (countTuple == null && groupcounter > 0) {
        System.out.println("LOCAL Count = " + groupcounter);
        countTuple = new Tuple();
        countTuple.setNumberAttribute("__count", groupcounter);
        if (lc != null) {
          lc.emit("***", countTuple);
        } else {
          System.out.println("\n\nLEADS COLLECTOR ==" + lc);
        }
      }
    }
    super.finalizeTask();
  }
}
