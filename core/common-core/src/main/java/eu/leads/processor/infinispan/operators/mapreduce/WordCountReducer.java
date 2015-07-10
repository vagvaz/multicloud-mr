package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsCombiner;

import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Apostolos Nydriotis on 2015/06/23.
 */
public class WordCountReducer extends LeadsCombiner<String, Tuple> {

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
    while (true) {
      try {
        Tuple input = iter.next();
        int count = Integer.valueOf(input.getAttribute("count"));
        sum += count;
      }catch(Exception e){
        if(e instanceof NoSuchElementException){
          break;
        }
        e.printStackTrace();
      }
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
