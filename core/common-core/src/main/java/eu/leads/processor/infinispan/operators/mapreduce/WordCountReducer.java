package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsReducer;

import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;

/**
 * Created by Apostolos Nydriotis on 2015/06/23.
 */
public class WordCountReducer extends LeadsReducer<String, Tuple> {

  public WordCountReducer(JsonObject configuration) {
    super(configuration);
  }

  public WordCountReducer(String configString) {
    super(configString);
  }

  @Override
  public Tuple reduce(String reducedKey, Iterator<Tuple> iter) {
    System.out.println(getClass().getName() + ".reduce!");
    return null;
  }
}
