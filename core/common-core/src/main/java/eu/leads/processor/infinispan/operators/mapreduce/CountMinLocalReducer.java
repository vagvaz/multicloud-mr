package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsReducer;

import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;

/**
 * Created by Apostolos Nydriotis on 2015/07/03.
 */
public class CountMinLocalReducer extends LeadsReducer<String, Tuple> {

  public CountMinLocalReducer(JsonObject configuration) {
    super(configuration);
  }

  public CountMinLocalReducer(String configuration) {
    super(configuration);
  }

  @Override
  public void reduce(String reducedKey, Iterator<Tuple> iter, LeadsCollector collector) {
    System.out.println(getClass().getName() + ".reduce local!");
    int sum = 0;
    while (iter.hasNext()) {
      Tuple input = iter.next();
      sum += Integer.valueOf(input.getAttribute("count"));
    }
    Tuple output = new Tuple();
    output.setAttribute("coord", reducedKey);
    output.setAttribute("sum", sum);
    String row = reducedKey.split(",")[0];
    collector.emit(row, output);
  }
}
