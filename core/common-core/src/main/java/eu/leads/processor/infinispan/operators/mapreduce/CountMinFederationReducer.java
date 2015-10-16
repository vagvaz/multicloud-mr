package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsReducer;

import org.mapdb.DBMaker;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Apostolos Nydriotis on 2015/07/03.
 */
public class CountMinFederationReducer extends LeadsReducer<String, Tuple> {

  transient int w;
  transient private LeadsCollector collector;
  transient private boolean collectorInitialized;
  transient private Map<String, Row> storage;

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

      Row r;
      if (!storage.containsKey(reducedKey)) {
        r = new Row(w);
        for (int i = 0; i < w; i++) {
          r.setElement(i, singleRow[i]);
        }
      } else {
        r = storage.get(reducedKey);
        for (int i = 0; i < w; i++) {
          r.setElement(i, r.getElement(i) + singleRow[i]);
        }
      }
      storage.put(reducedKey, r);

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
      storage = DBMaker.tempTreeMap();
    }
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");

    if (isComposable) {
      for (Map.Entry<String, Row> entry : storage.entrySet()) {
        String singleRowStr = "";
        for (int i = 0; i < w; i++) {
          singleRowStr += String.valueOf(entry.getValue().getElement(i));
          if (i < w - 1) {
            singleRowStr += ",";
          }
        }
        collector.emit(entry.getKey(), singleRowStr);
      }
    }
  }

  private class Row implements Serializable {
    private int[] row;

    public Row(int width) {
      row = new int[width];
    }

    public int getElement(int position) {
      return row[position];
    }

    public void setElement(int position, int element) {
      row[position] = element;
    }
  }
}
