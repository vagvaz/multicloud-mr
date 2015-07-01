package eu.leads.processor.infinispan;

import java.io.Serializable;

/**
 * Created by Apostolos Nydriotis on 2015/06/24.
 */
public class LeadsLocalReducerCallable<kOut, vOut> extends LeadsBaseCallable<kOut, Object>
    implements
    Serializable {

  private static final long serialVersionUID = 8028728191155715526L;
  private LeadsReducer<kOut, vOut> reducer = null;
  private LeadsCollector collector;
  private String prefix;
  private String site;

  public LeadsLocalReducerCallable(String cacheName, LeadsReducer<kOut, vOut> reducer,
                                   String prefix, String site) {
    super("{}", cacheName);
    this.reducer = reducer;
    collector = new LeadsCollector(1000, cacheName);
    collector.setOnMap(true);
    this.prefix = prefix;
    this.site = site;
  }

  @Override
  public void executeOn(kOut key, Object value) {
    LeadsIntermediateIterator<vOut> values = new LeadsIntermediateIterator<>((String) key, prefix,
                                                                             imanager);
    reducer.reduce(key, values, collector);
  }

  @Override
  public void initialize() {
    super.initialize();
    collector.setOnMap(true);
    collector.setEmanager(emanager);
    collector.setSite(site);
    collector.setManager(embeddedCacheManager);
    collector.initializeCache(inputCache.getName(), imanager);

    this.reducer.initialize();
  }

  @Override
  public void finalizeCallable() {
    reducer.finalizeTask();
    super.finalizeCallable();
  }
}