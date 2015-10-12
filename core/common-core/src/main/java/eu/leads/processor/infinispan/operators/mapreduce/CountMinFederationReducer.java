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
  }

  public CountMinFederationReducer(String configString) {
    super(configString);
  }

  @Override public void reduce(String reducedKey, Iterator<Tuple> iter, LeadsCollector collector) {
    int[] singleRow = new int[w];
    while (iter.hasNext()) {
      Tuple t = iter.next();
      String coord = t.getAttribute("coord");
      int sum = t.getNumberAttribute("sum").intValue();
      int column = Integer.valueOf(coord.split(",")[1]);
      singleRow[column] += sum;
    }

    Tuple output = new Tuple();
    String singleRowStr = "";
    for (int i = 0; i < singleRow.length; i++) {
      singleRowStr += String.valueOf(singleRow[i]);
      if (i < singleRow.length - 1) {
        singleRowStr += ",";
      }
    }
    output.setAttribute("singleRow", singleRowStr);
    collector.emit(reducedKey, output);
  }

  @Override public void initialize() {
    super.initialize();
    w = conf.getInteger("w");
  }

  @Override protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");
  }
}
