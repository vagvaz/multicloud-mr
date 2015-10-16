package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsCombiner;
import eu.leads.processor.infinispan.LeadsReducer;

import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Apostolos Nydriotis on 2015/07/03.
 */
public class CountMinLocalReducer extends LeadsCombiner<String, Tuple> {

  transient private LeadsCollector collector;
  transient private boolean collectorInitialized;
  transient private Map<String, Integer> storage;  // TODO(ap0n): Use mapDb

  public CountMinLocalReducer() {
    super();
  }

  public CountMinLocalReducer(JsonObject configuration) {
    super(configuration);
  }

  public CountMinLocalReducer(String configuration) {
    super(configuration);
  }

  @Override
  public void initialize() {
    super.initialize();
    collectorInitialized = false;
    if (isComposable) {
      storage = new HashMap<>();
    }
  }

  @Override
  public void reduce(String reducedKey, Iterator<Tuple> iter, LeadsCollector collector) {

    int sum = 0;
    while (iter.hasNext()) {
      Tuple t = iter.next();
      sum += t.getNumberAttribute("count").intValue();
    }
      Tuple output = new Tuple();
      output.setAttribute("coord", reducedKey);
      output.setAttribute("sum", sum);
      String row = reducedKey.split(",")[0];
      collector.emit(row, output);
//    }
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");

  }
}
