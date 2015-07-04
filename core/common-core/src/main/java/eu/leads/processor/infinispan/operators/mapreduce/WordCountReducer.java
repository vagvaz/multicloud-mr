package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsReducer;

import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;

/**
 * Created by Apostolos Nydriotis on 2015/06/23.
 */
public class WordCountReducer extends LeadsReducer<String, Tuple> {

  public WordCountReducer(JsonObject configuration) {
    super(configuration);
  }

  public WordCountReducer(String configString) {
    super(configString);
  }

  @Override
  public void reduce(String reducedKey, Iterator<Tuple> iter, LeadsCollector collector) {
//    System.out.println(getClass().getName() + ".reduce!");
    int sum = 0;
    while (iter.hasNext()) {
      Tuple input = iter.next();
      int count = Integer.valueOf(input.getAttribute("count"));
      sum += count;
    }
    Tuple output = new Tuple();
    output.setAttribute("count", sum);
    collector.emit(reducedKey, output);
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");
  }
}
