package eu.leads.processor.infinispan.operators;

import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;

/**
 * Created by vagvaz on 2/20/15.
 */
public class ProjectCallableUpdated<K, V> extends LeadsSQLCallable<K, V> {
  transient private String prefix = "";

  public ProjectCallableUpdated() {
    super();
  }

  public ProjectCallableUpdated(String configString, String output) {
    super(configString, output);
  }

  @Override public void initialize() {
    super.initialize();
    prefix = conf.getString("output") + ":";
    collector.setOnMap(false);
    collector.setManager(this.embeddedCacheManager);
    collector.setEmanager(emanager);
    collector.setSite(LQPConfiguration.getInstance().getMicroClusterName());
    collector.initializeCache(inputCache.getName(), imanager);
  }

  @Override public void executeOn(K ikey, V ivalue) {
    String key = (String) ikey;
    //    String value = (String)ivalue;
    String tupleId = key.substring(key.indexOf(':') + 1);
    //    Tuple projected = new Tuple(value);
    Tuple projected = (Tuple) ivalue;
    //    handlePagerank(projected);
    projected = prepareOutput(projected);
    //    outputCache.put(prefix + tupleId, projected.asString());
    collector.emit(prefix + tupleId, projected);
  }
}
