package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsCombiner;

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
    int documentsCount = 0;
    Map<String, Integer> dimentions = new HashMap<>();
    while (iter.hasNext()) {
      documentsCount++;  // TODO(ap0n): If combine is active this should be
                         // TODO        documentsCount += tuple.documentsCount from the combiner
      Tuple t = iter.next();
      for (String s : t.getFieldNames()) {
        int wordFrequency = t.getNumberAttribute(s).intValue();
        Integer currentFrequency = dimentions.get(s);
        if (currentFrequency == null) {
          dimentions.put(s, wordFrequency);
        } else {
          dimentions.put(s, currentFrequency + wordFrequency);
        }
      }
    }

    for (Map.Entry<String, Integer> entry : dimentions.entrySet()) {
      entry.setValue(entry.getValue() / documentsCount);
    }
    Tuple result = new Tuple();
    result.asBsonObject().putAll(dimentions);
    collector.emit(reducedKey, result);
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");
  }
}
