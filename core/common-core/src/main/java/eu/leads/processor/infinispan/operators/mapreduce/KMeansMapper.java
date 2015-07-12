package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsMapper;

import org.infinispan.distexec.mapreduce.Collector;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Apostolos Nydriotis on 2015/07/10.
 */
public class KMeansMapper extends LeadsMapper<String, Tuple, String, Tuple> {

  int k;
  Map<String, Double>[] centers;
  Double[] norms;

  public KMeansMapper(JsonObject configuration) {
    super(configuration);
  }

  public KMeansMapper(String configString) {
    super(configString);
  }

  @Override
  public void map(String key, Tuple value, Collector<String, Tuple> collector) {
    System.out.println("MAPPER");
    double maxSimilarity = 0;
    int index = -1;

    for (int i = 0; i < k; i++) {
      double d = calculateDistance(i, value);
      if (d > maxSimilarity) {
        maxSimilarity = d;
        index = i;
      }
    }
    Tuple res = new Tuple();
    res.asBsonObject().put("value", value.asBsonObject());
    res.setAttribute("count", 1d);
    collector.emit(String.valueOf(index), res);
  }

  @Override
  public void initialize() {
    super.initialize();
    k = conf.getInteger("k");

    // Get centers from configuration
    centers = new Map[k];
    norms = new Double[k];

    for (int i = 0; i < k; i++) {
      norms[i] = conf.getNumber("norm" + String.valueOf(i)).doubleValue();
      Map<String, Double> map = new HashMap<>();
      JsonObject doc = conf.getField("center" + String.valueOf(i));
      for (String word : doc.getFieldNames()) {
        map.put(word, doc.getNumber(word).doubleValue());
      }
      centers[i] = map;
    }
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");
  }

  private double calculateDistance(int i, Tuple value) {

    // cosine = A B / ||A|| ||B|| (Ignore document's norm)

    Double numerator = 0d;

    Map<String, Double> document = new HashMap<>();
    for (String s : value.getFieldNames()) {
      document.put(s, value.getNumberAttribute(s).doubleValue());
    }

    for (Map.Entry<String, Double> entry : document.entrySet()) {
      Double centroidValue = centers[i].get(entry.getKey());
      if (centroidValue != null) {
        numerator += entry.getValue() * centroidValue;
      }
    }

    return numerator / Math.sqrt(norms[i]);
  }
}
