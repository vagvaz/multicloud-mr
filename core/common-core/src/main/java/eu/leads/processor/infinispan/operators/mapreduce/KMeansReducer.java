package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsCombiner;

import org.bson.BasicBSONObject;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Apostolos Nydriotis on 2015/07/10.
 */
public class KMeansReducer extends LeadsCombiner<String, Tuple> {

  public KMeansReducer(JsonObject configuration) {
    super(configuration);
  }

  public KMeansReducer(String configString) {
    super(configString);
  }

  @Override
  public void reduce(String reducedKey, Iterator<Tuple> iter, LeadsCollector collector) {
    System.out.println("REDUCER");
    int documentsCount = 0;
    Map<String, Double> dimensions = new HashMap<>();

    String clusterDocuments = "";  // Space-separated document ids

    while (iter.hasNext()) {
      Tuple t = iter.next();

      Tuple dimensionsTuple = new Tuple((BasicBSONObject) t.getGenericAttribute("dimensions"));
      int count = t.getNumberAttribute("documentsCount").intValue();
      documentsCount += count;

      for (String s : dimensionsTuple.getFieldNames()) {
        Double wordFrequency = dimensionsTuple.getNumberAttribute(s).doubleValue();
        Double currentFrequency = dimensions.get(s);
        if (currentFrequency == null) {
          dimensions.put(s, wordFrequency);
        } else {
          dimensions.put(s, currentFrequency + wordFrequency);
        }
      }
      clusterDocuments += t.getAttribute("clusterDocuments");
    }

    double norm = 0d;
    for (Map.Entry<String, Double> entry : dimensions.entrySet()) {
      entry.setValue(entry.getValue() / (double) documentsCount);
      norm += entry.getValue() * entry.getValue();
    }

    Tuple newCentroidTuple = new Tuple();
    newCentroidTuple.asBsonObject().putAll(dimensions);

    Tuple r = new Tuple();
    r.asBsonObject().put("newCentroid", newCentroidTuple.asBsonObject());
    r.setAttribute("cluster" + reducedKey, clusterDocuments);
    r.setAttribute("norm" + reducedKey, norm);
    collector.emit(reducedKey, r);
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");
  }
}
