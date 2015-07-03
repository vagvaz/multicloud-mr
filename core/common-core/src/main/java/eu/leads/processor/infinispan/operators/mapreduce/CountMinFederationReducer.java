package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsReducer;

import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;

/**
 * Created by Apostolos Nydriotis on 2015/07/03.
 */
public class CountMinFederationReducer extends LeadsReducer<String, Tuple> {

  int w;

  public CountMinFederationReducer(JsonObject configuration) {
    super(configuration);
    // TODO(ap0n): Init w for configuration
  }

  public CountMinFederationReducer(String configString) {
    super(configString);
    // TODO(ap0n): Init w for configuration
  }

  @Override
  public void reduce(String reducedKey, Iterator<Tuple> iter, LeadsCollector collector) {
    System.out.println(getClass().getName() + ".reduce global!");
    int[] singleRow = new int[w];
    while (iter.hasNext()) {
      String coord = iter.next().getAttribute("coord");
      int column = Integer.valueOf(coord.split(",")[1]);
      singleRow[column]++;
    }

    Tuple output = new Tuple();
    output.setAttribute("singleRow", singleRow);
    collector.emit(reducedKey, output);
  }
}
