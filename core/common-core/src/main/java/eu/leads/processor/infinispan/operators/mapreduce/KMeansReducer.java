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
    int valuesReduced = 0;

    while (iter.hasNext()) {
      Tuple t = iter.next();

      Tuple valueTuple = new Tuple((BasicBSONObject) t.getGenericAttribute("value"));
      int count = t.getNumberAttribute("count").intValue();

      documentsCount += count;
      for (String s : valueTuple.getFieldNames()) {
        Double wordFrequency = valueTuple.getNumberAttribute(s).doubleValue();
        Double currentFrequency = dimensions.get(s);
        if (currentFrequency == null) {
          dimensions.put(s, wordFrequency);
        } else {
          dimensions.put(s, currentFrequency + wordFrequency);
        }
      }
      valuesReduced++;
    }
    double norm = 0d;
    for (Map.Entry<String, Double> entry : dimensions.entrySet()) {

      entry.setValue(entry.getValue() / (double) documentsCount);
      norm += entry.getValue() * entry.getValue();
    }

    Tuple dimensionsTuple = new Tuple();
    dimensionsTuple.asBsonObject().putAll(dimensions);

    Tuple r = new Tuple();
    r.asBsonObject().put("value", dimensionsTuple.asBsonObject());
    r.setAttribute("count", valuesReduced);
    r.setAttribute("norm" + reducedKey, norm);
    collector.emit(reducedKey, r);
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");
  }
}
