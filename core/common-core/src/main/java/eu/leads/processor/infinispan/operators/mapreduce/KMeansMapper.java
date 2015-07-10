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
  Map<String, Integer>[] centers;

  public KMeansMapper(JsonObject configuration) {
    super(configuration);
  }

  public KMeansMapper(String configString) {
    super(configString);
  }

  @Override
  public void map(String key, Tuple value, Collector<String, Tuple> collector) {
    double maxSimilarity = 0;
    int index = -1;

    for (int i = 0; i < k; i++) {
      double d = calculateDistance(i, value);
      if (d > maxSimilarity) {
        maxSimilarity = d;
        index = i;
      }
    }
    collector.emit(String.valueOf(index), value);
  }

  @Override
  public void initialize() {
    super.initialize();
    k = conf.getInteger("k");

    // Get centers from configuration
    centers = new Map[k];
    for (int i = 0; i < k; i++) {
      Map<String, Integer> map = new HashMap<>();
      Tuple doc = conf.getField("doc" + String.valueOf(i));
      for (String word : doc.getFieldNames()) {
        map.put(word, doc.getNumberAttribute(word).intValue());
      }
      centers[i] = map;
    }
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");
  }

  private double calculateDistance(int i, Tuple value) {

    // cosine = A B / ||A|| ||B||

    Integer numerator = 0;
    double normA = 0.0, normB = 0.0;

    for (Map.Entry<String, Integer> entry : centers[i].entrySet()) {
      if (value.hasField(entry.getKey())) {
        numerator += (value.getNumberAttribute(entry.getKey()).intValue() * entry.getValue());
      }
      normA += entry.getValue() ^ 2;
    }

    for (String s : value.getFieldNames()) {
      normB += value.getNumberAttribute(s).intValue() ^ 2;
    }

    return numerator / Math.sqrt(normA) / Math.sqrt(normB);
  }
}
