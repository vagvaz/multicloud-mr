package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.common.infinispan.ClusterInfinispanManager;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsReducer;
import eu.leads.processor.math.MathUtils;
import org.infinispan.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.*;

//import eu.leads.processor.common.utils.SQLUtils;


/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 9:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class GroupByReducer extends LeadsReducer<String, Tuple> {



  transient Cache<String, String> data;
  transient String prefix;
  transient List<Object> aggregateValues;
  transient List<String> functionType;
  transient List<String> columnTypes;
  transient List<String> columnParams;
  transient private ArrayList<String> groupByColumns;
  transient private ArrayList<JsonObject> aggregates;
  transient Map<String, Integer> typesOfaggregates;
  transient List<String> aggregateNames;
  transient Set<String> inputFields;
  transient ArrayList<String> aggregateInferred;
  transient Logger log = null;
  transient ProfileEvent groupReducerEvent;
  transient ProfileEvent reducerEvent;
  transient boolean isCountStar = false;

  public GroupByReducer() {
  }

  public GroupByReducer(JsonObject configuration) {
    super(configuration);
  }

  public GroupByReducer(String configString) {
    super(configString);
  }

  @Override public void initialize() {

    isInitialized = true;
    super.initialize();
    log = LoggerFactory.getLogger(GroupByReducer.class);
    imanager = new ClusterInfinispanManager(manager);
    prefix = outputCacheName + ":";
    data = (Cache<String, String>) InfinispanClusterSingleton.getInstance().getManager()
        .getPersisentCache(outputCacheName);
    aggregateValues = new ArrayList<>();
    functionType = new ArrayList<>();
    columnTypes = new ArrayList<>();
    columnParams = new ArrayList<>();
    typesOfaggregates = new HashMap<>();
    aggregateNames = new ArrayList<>();
    aggregates = new ArrayList<>();
    JsonArray columns = conf.getObject("body").getArray("groupingKeys");
    Iterator<Object> columnIterator = columns.iterator();
    groupByColumns = new ArrayList<>(columns.size());

    while (columnIterator.hasNext()) {
      JsonObject columnObject = (JsonObject) columnIterator.next();
      groupByColumns.add(columnObject.getString("name"));
    }
    if (groupByColumns.size() == 0)
      isCountStar = true;
    JsonArray functions = conf.getObject("body").getArray("aggrFunctions");
    Iterator<Object> funcIterator = functions.iterator();
    while (funcIterator.hasNext()) {
      JsonObject current = (JsonObject) funcIterator.next();
      aggregates.add(current);
      String funcType = current.getObject("funcDesc").getObject("signature").getString("name");

      JsonObject argument = null;
      if (current.getArray("argEvals").size() > 0) {
        //         if( ((JsonObject) current).getArray("argEvals").size() > 0){
        argument = current.getArray("argEvals").get(0);
      } else { // handling empty parameters for count(*)
        argument = new JsonObject();
        argument.putString("type", "FIELD");
        JsonObject body = new JsonObject();
        body.putNumber("fieldId", -1);
        body.putString("type", "FIELD");
        JsonObject columnObject = new JsonObject();
        columnObject.putString("name", "*");
        columnObject.putObject("dataType", new JsonObject().putString("type", "INT8"));
        body.putObject("column", columnObject);
        argument.putObject("body", body);
      }
      columnParams.add(argument.getObject("body").getObject("column").getString("name"));
      functionType.add(funcType);
      Integer count = typesOfaggregates.get(funcType);
      if (count == null) {
        aggregateNames.add("?" + funcType);
        count = 1;
      } else {
        aggregateNames.add("?" + funcType + "_" + count.toString());
        count++;
      }
      typesOfaggregates.put(funcType, count);
      JsonArray params = ((JsonObject) current.getObject("funcDesc")).getObject("signature").getArray("paramTypes");
      JsonObject parameter = null;
      if (params.size() > 0) {
        parameter = params.get(0);
        columnTypes.add(parameter.getString("type"));
        Object object = MathUtils.getInitialValue(parameter.getString("type"),
            current.getObject("funcDesc").getObject("signature").getString("name"));
        aggregateValues.add(object);
      } else {
        //Should be done only for count
        columnTypes.add("INT8");
        Object object =
            MathUtils.getInitialValue("INT8", current.getObject("funcDesc").getObject("signature").getString("name"));
        aggregateValues.add(object);
      }
    }
    aggregateInferred = inferFinalAggNames();
    reducerEvent = new ProfileEvent("reduceEvent", log);
    groupReducerEvent = new ProfileEvent("groupReducer", log);
  }

  private ArrayList<String> inferFinalAggNames() {
    ArrayList<String> result = new ArrayList<>(aggregateNames.size());
    Iterator<Object> inputIterator = conf.getObject("body").getObject("inputSchema").getArray("fields").iterator();
    Set<String> inputColumns = new HashSet<String>();
    while (inputIterator.hasNext()) {
      JsonObject field = (JsonObject) inputIterator.next();
      inputColumns.add(field.getString("name"));
    }
    Iterator<Object> targetIterator = conf.getObject("body").getArray("targets").iterator();
    int counter = 0;
    while (targetIterator.hasNext()) {
      JsonObject target = (JsonObject) targetIterator.next();
      String targetName = target.getObject("expr").getObject("body").getObject("column").getString("name");

      if (!inputColumns.contains(targetName)) {
        result.add(targetName);
        counter++;
      }
    }

    return result;
  }

  @Override public void reduce(String key, Iterator<Tuple> iterator, LeadsCollector collector) {
    //Reduce takes all the grouped Typles per key
    reducerEvent.start("reduceEvent_1");
    if (key == null || key.equals("")) {
      log.error("reduce called with null key? " + (key == null));
      return;
    }
    //      log.error("Computing values for group " + key + " .");
    //      if (!isInitialized) initialize();
    resetValues();
    Tuple t = null;
    //        progress();
    //Iterate overall values
    int tuplecounter = 0;
    reducerEvent.end();
    while (true) {
      try {
        reducerEvent.start("reduceEvent_2");
        tuplecounter++;
        Tuple itTuple = iterator.next();
        if (itTuple == null) {
          System.err.println("Iterator got next and it was null..." + iterator.toString());
          log.error("Iterator got next and it was null..." + iterator.toString());
          continue;
        }
        t = (itTuple);
        reducerEvent.end();
        Iterator<String> funcTypeIterator = functionType.iterator();
        //           Iterator<Object> aggValuesIterator = aggregateValues.iterator();
        Iterator<String> columnTypesIterator = columnTypes.iterator();
        Iterator<String> columnNameiterator = columnParams.iterator();
        int counter = 0;
        //for each function
        while (funcTypeIterator.hasNext()) {

          String funcType = funcTypeIterator.next();
          String columnType = columnTypesIterator.next();
          String column = columnNameiterator.next();
          //set new aggvalue according to function type, columnt Type, old agg value, currentValue
          if (funcType.equals("count") && isCountStar) {
            aggregateValues.set(counter,
                ((Number) aggregateValues.get(counter)).longValue() + ((Number) t.getGenericAttribute("__count"))
                    .longValue());
          } else {
            aggregateValues.set(counter, MathUtils
                .updateFunctionValue(funcType, columnType, aggregateValues.get(counter),
                    t.getGenericAttribute(column)));
          }
          counter++; //inc counter for the next agg value
        }

      } catch (Exception e) {
        if (e instanceof NoSuchElementException) {
          //               log.info("End of leadsIntermidateIterator " + e.getMessage());
          break;
        }
        if (e instanceof InterruptedException) {
          //profilerLog.error(this.imanager.getCacheManager().getAddress().toString() + " was interrupted ");
          log.error(this.imanager.getCacheManager().getAddress().toString() + " was interrupted ");
          throw e;
        }
        log.error("EXCEPTION WHILE updating agg value");
        log.error(e.getClass() + " " + e.getMessage());
        log.error(iterator.toString());
        //            log.error(t.toString());
        break;
      }

    }

    Iterator<String> nameIterator = aggregateInferred.iterator();
    Iterator<Object> aggValuesIterator = aggregateValues.iterator();
    Iterator<String> funcTypeIterator = functionType.iterator();
    //      log.error("Computing final agg values for group " + key + " after " + tuplecounter + " tuples");
    //compute final values and put agg values to tuple
    try {
      while (nameIterator.hasNext()) {
        String name = nameIterator.next();
        Object value = aggValuesIterator.next();
        String funcType = funcTypeIterator.next();
        Object tupleValue = value;
        //if function is avg compute value
        if (funcType.equals("avg")) {
          Map<String, Object> avgMap = (Map<String, Object>) value;
          Double avgValue = MathUtils.computeAvg(avgMap);
          tupleValue = avgValue;
        }
        t.setAttribute(name, tupleValue);
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        //profilerLog.error(this.imanager.getCacheManager().getAddress().toString() + " was interrupted ");
        log.error(this.imanager.getCacheManager().getAddress().toString() + " was interrupted ");
        throw e;
      }
      log.error("EXCEPTION ON FINAL setting agg value");
      log.error(e.getClass() + " " + e.getMessage());
      log.error(iterator.toString());
      //            log.error(t.toString());
      reducerEvent.end();
      return;

    }
    //prepare output
    //        System.err.println("t: " + t.toString());
    t = prepareOutput(t);
    //        System.err.println("tout: " + t.toString());
    //        collector.emit(prefix + key, t.asString());
    collector.emit(prefix + key, t);
    return;
  }



  private void resetValues() {
    for (int i = 0; i < aggregateValues.size(); i++) {
      aggregateValues.set(i, MathUtils.getInitialValue(columnTypes.get(i), functionType.get(i)));
    }
  }

  @Override protected void finalizeTask() {
    groupReducerEvent.end();
    super.finalizeTask();
  }
}
