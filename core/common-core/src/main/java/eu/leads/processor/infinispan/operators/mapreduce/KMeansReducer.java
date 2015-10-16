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

  transient private Map<String, Tuple> storage;
  transient private LeadsCollector collector;
  transient private boolean collectorInitialized;

  public KMeansReducer() {
    super();
  }

  public KMeansReducer(JsonObject configuration) {
    super(configuration);
  }

  public KMeansReducer(String configString) {
    super(configString);
  }

  @Override
  public void initialize() {
    super.initialize();
    if (isComposable) {
      collectorInitialized = false;
      storage = new HashMap<>();
    }
  }

  @Override
  public void reduce(String reducedKey, Iterator<Tuple> iter, LeadsCollector collector) {
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

    if (isComposable) {

      if (!collectorInitialized) {
        this.collector = collector;
        collectorInitialized = true;
      }

      Tuple toStoreTuple = new Tuple();
      Tuple toStoreDimensionsTuple;
      if (storage.containsKey(reducedKey)) {
        Tuple t = storage.get(reducedKey);
        String storedClusterDocuments = t.getAttribute("clusterDocuments");
        int storedDocumentsCount = t.getNumberAttribute("documentsCount").intValue();
        Tuple storedDimensionsTuple =
            new Tuple((BasicBSONObject) t.getGenericAttribute("dimensions"));

        Map<String, Double> updatedDimensions = new HashMap<>();
        for (String key : storedDimensionsTuple.getFieldNames()) {
          updatedDimensions.put(key, storedDimensionsTuple.getNumberAttribute(key).doubleValue());
        }

        for (Map.Entry<String, Double> e : dimensions.entrySet()) {
          if (updatedDimensions.containsKey(e.getKey())) {
            updatedDimensions.put(e.getKey(), updatedDimensions.get(e.getKey()) + e.getValue());
          } else {
            updatedDimensions.put(e.getKey(), e.getValue());
          }
        }
        toStoreDimensionsTuple = new Tuple();
        toStoreDimensionsTuple.asBsonObject().putAll(updatedDimensions);
        toStoreTuple.setNumberAttribute("documentsCount", documentsCount + storedDocumentsCount);
        toStoreTuple.setAttribute("clusterDocuments", clusterDocuments
                                                      + t.getAttribute("clusterDocuments"));

      } else {  // not exists in storage
        toStoreDimensionsTuple = new Tuple();
        toStoreDimensionsTuple.asBsonObject().putAll(dimensions);
        toStoreTuple.setNumberAttribute("documentsCount", documentsCount);
        toStoreTuple.setAttribute("clusterDocuments", clusterDocuments);
      }

      toStoreTuple.asBsonObject().put("dimensions", toStoreDimensionsTuple.asBsonObject());
      storage.put(reducedKey, toStoreTuple);

    } else {  // not composable
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
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");
    for (Map.Entry<String, Tuple> e : storage.entrySet()) {
      String reducedKey = e.getKey();
      Tuple storedTuple = e.getValue();

      int documentsCount = storedTuple.getNumberAttribute("documentsCount").intValue();
      Tuple storedDimensionsTuple =
          new Tuple((BasicBSONObject) storedTuple.getGenericAttribute("dimensions"));

      Map<String, Double> dimensions = new HashMap<>();
      for (String key : storedDimensionsTuple.getFieldNames()) {
        dimensions.put(key, storedDimensionsTuple.getNumberAttribute(key).doubleValue());
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
      r.setAttribute("cluster" + reducedKey, storedTuple.getAttribute("clusterDocuments"));
      r.setAttribute("norm" + reducedKey, norm);
      collector.emit(reducedKey, r);
    }
  }
}
