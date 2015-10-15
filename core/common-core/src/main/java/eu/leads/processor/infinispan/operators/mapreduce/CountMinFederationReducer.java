package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsReducer;

import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Apostolos Nydriotis on 2015/07/03.
 */
public class CountMinFederationReducer extends LeadsReducer<String, Tuple> {

  transient int w;
  transient private LeadsCollector collector;
  transient private boolean collectorInitialized;
  transient private Map<String, Map<Integer, Integer>> storage;  // TODO(ap0n): Use mapDb

  public CountMinFederationReducer() {
    super();
  }


  public CountMinFederationReducer(JsonObject configuration) {
    super(configuration);
  }

  public CountMinFederationReducer(String configString) {
    super(configString);
  }

  @Override
  public void reduce(String reducedKey, Iterator<Tuple> iter, LeadsCollector collector) {

    int[] singleRow = new int[w];

    while (iter.hasNext()) {
      Tuple t = iter.next();
      String coord = t.getAttribute("coord");
      int sum = t.getNumberAttribute("sum").intValue();
      int column = Integer.valueOf(coord.split(",")[1]);
      singleRow[column] += sum;
    }

    if (isComposable) {
      if (!collectorInitialized) {
        this.collector = collector;
        collectorInitialized = true;
      }
      if (!storage.containsKey(reducedKey)) {
        Map<Integer, Integer> v = new HashMap<>();
        for (int i = 0; i < w; i++) {
          v.put(i, singleRow[i]);
        }
        storage.put(reducedKey, v);
      } else {
        Map<Integer, Integer> storedRow = storage.get(reducedKey);
        for (int i = 0; i < w; i++) {
          storedRow.put(i, storedRow.get(i) + singleRow[i]);
        }
        storage.put(reducedKey, storedRow);
      }
    } else {
      String singleRowStr = "";
      for (int i = 0; i < w; i++) {
        singleRowStr += String.valueOf(singleRow[i]);
        if (i < singleRow.length - 1) {
          singleRowStr += ",";
        }
      }
      collector.emit(reducedKey, singleRowStr);
    }
  }

  @Override
  public void initialize() {
    super.initialize();
    w = conf.getInteger("w");
    collectorInitialized = false;
    if (isComposable) {
      storage = new HashMap<>();
    }
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");

    if (isComposable) {
      for (Map.Entry<String, Map<Integer, Integer>> entry : storage.entrySet()) {
        String singleRowStr = "";
        for (int i = 0; i < w; i++) {
          singleRowStr += String.valueOf(entry.getValue().get(i));
          if (i < w - 1) {
            singleRowStr += ",";
          }
        }
        collector.emit(entry.getKey(), singleRowStr);
      }
    }
  }
}
