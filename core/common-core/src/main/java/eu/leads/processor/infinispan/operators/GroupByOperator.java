package eu.leads.processor.infinispan.operators;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.infinispan.operators.mapreduce.GroupByMapper;
import eu.leads.processor.infinispan.operators.mapreduce.GroupByReducer;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 1:19 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect
//@JsonDeserialize(converter = GroupByJsonDelegate.class)
public class GroupByOperator extends MapReduceOperator {


  List<String> groupByColumns;

  public GroupByOperator(Node com, InfinispanManager persistence, LogProxy log, Action action) {

    super(com, persistence, log, action);

    JsonArray columns = conf.getObject("body").getArray("groupingKeys");
    Iterator<Object> columnIterator = columns.iterator();
    groupByColumns = new ArrayList<>(columns.size());

    while (columnIterator.hasNext()) {
      JsonObject columnObject = (JsonObject) columnIterator.next();
      groupByColumns.add(columnObject.getString("name"));
    }

    JsonArray functions = conf.getObject("body").getArray("aggrFunctions");
    Iterator<Object> funcIterator = functions.iterator();
    List<JsonObject> aggregates = new ArrayList<>(functions.size());
    while (funcIterator.hasNext()) {
      aggregates.add((JsonObject) funcIterator.next());
    }
  }

  @Override public void init(JsonObject config) {
    super.init(conf);
    setMapper(new GroupByMapper(conf.toString()));
    setReducer(new GroupByReducer(conf.toString()));
    init_statistics(this.getClass().getCanonicalName());
  }

  @Override public void execute() {
    super.execute();

  }

  @Override public void cleanup() {
    super.cleanup();
  }

  @Override public String getContinuousListenerClass() {
    return null;
  }


  @Override public void setupMapCallable() {
    //      init(conf);
    setMapper(new GroupByMapper(conf.toString()));
    super.setupMapCallable();
  }

  @Override public void setupReduceCallable() {
    setReducer(new GroupByReducer(conf.toString()));
    super.setupReduceCallable();
  }


}
