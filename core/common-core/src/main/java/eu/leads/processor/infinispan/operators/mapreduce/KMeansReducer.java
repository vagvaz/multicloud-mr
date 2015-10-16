package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsCombiner;

import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.mapdb.DBMaker;
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
      storage = DBMaker.tempTreeMap();
    }
  }

  @Override
  public void reduce(String reducedKey, Iterator<Tuple> iter, LeadsCollector collector) {
    int documentsCount = 0;
    BasicBSONObject dimensions = new BasicBSONObject();

    String clusterDocuments = "";  // Space-separated document ids

    while (iter.hasNext()) {
      Tuple t = iter.next();
      BasicBSONObject newDimensions = (BasicBSONObject) t.getGenericAttribute("dimensions");
      int count = t.getNumberAttribute("documentsCount").intValue();
      documentsCount += count;

      for (Map.Entry e : newDimensions.entrySet()) {
        Double wordFrequency = (Double) e.getValue();
        Double currentFrequency = (Double) dimensions.get(e.getKey());
        if (currentFrequency == null) {
          dimensions.put((String) e.getKey(), wordFrequency);
        } else {
          dimensions.put((String) e.getKey(), currentFrequency + wordFrequency);
        }
      }

      clusterDocuments += t.getAttribute("clusterDocuments");
    }

    if (isComposable) {

      if (!collectorInitialized) {
        this.collector = collector;
        collectorInitialized = true;
      }

      Tuple t = storage.get(reducedKey);
      if (t != null) {

        int storedDocumentsCount = t.getNumberAttribute("documentsCount").intValue();
        BasicBSONObject storedDimensions = (BasicBSONObject) t.getGenericAttribute("dimensions");

        for (Map.Entry e : dimensions.entrySet()) {
          Double storedFrequency = (Double) storedDimensions.get(e.getKey());
          Double newFrequency = (Double) e.getValue();
          String key = (String) e.getKey();
          if (storedFrequency == null) {
            storedDimensions.put(key, newFrequency);
          } else {
            storedDimensions.put(key, storedFrequency + newFrequency);
          }
        }

        t.setNumberAttribute("documentsCount", documentsCount + storedDocumentsCount);
        t.setAttribute("clusterDocuments", clusterDocuments + t.getAttribute("clusterDocuments"));
        t.setAttribute("dimensions", storedDimensions);

      } else {  // not exists in storage
        t = new Tuple();
        t.setNumberAttribute("documentsCount", documentsCount);
        t.setAttribute("clusterDocuments", clusterDocuments);
        t.setAttribute("dimensions", dimensions);
      }

      storage.put(reducedKey, t);

    } else {  // not composable
      double norm = 0d;
      for (Map.Entry entry : dimensions.entrySet()) {
        Double frequency = (Double) entry.getValue();
        entry.setValue(frequency / (double) documentsCount);
        norm += frequency * frequency;
      }

      Tuple r = new Tuple();
      r.setAttribute("newCentroid", dimensions);
      r.setAttribute("cluster" + reducedKey, clusterDocuments);
      r.setAttribute("norm" + reducedKey, norm);
      System.out.println("\n\n\nEMITTING REDUCED KEY: " + reducedKey + "\n\n\n");
      collector.emit(reducedKey, r);
    }
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");
    if (isComposable) {
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
}
