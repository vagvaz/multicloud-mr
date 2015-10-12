package eu.leads.processor.infinispan.operators;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.math.FilterOperatorTree;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;

/**
 * Created by vagvaz on 2/20/15.
 */
public class FilterCallableUpdated<K, V> extends LeadsSQLCallable<K, V> implements Serializable {

  protected String qualString;
  transient protected FilterOperatorTree tree;

  public FilterCallableUpdated() {
    super();
  }

  public FilterCallableUpdated(String configString, String output, String qualString) {
    super(configString, output);
    this.qualString = qualString;
  }

  @Override public void initialize() {
    super.initialize();
    JsonObject object = new JsonObject(qualString);
    tree = new FilterOperatorTree(object);
  }

  @Override public void executeOn(K ikey, V ivalue) {
    String key = (String) ikey;
    //    Tuple tuple = new Tuple(value);
    Tuple tuple = (Tuple) ivalue;
    if (tree.accept(tuple)) {
      tuple = prepareOutput(tuple);
      outputCache.put(key, tuple);
    }
  }
}
