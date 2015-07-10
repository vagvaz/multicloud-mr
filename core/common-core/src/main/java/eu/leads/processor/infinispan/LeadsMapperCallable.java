package eu.leads.processor.infinispan;

import org.infinispan.Cache;

import java.io.Serializable;
import java.util.Set;

public class LeadsMapperCallable<K, V, kOut, vOut> extends LeadsBaseCallable<K, V> implements

                                                                                   Serializable {

  /**
   * tr
   */
  private static final long serialVersionUID = 1242145345234214L;

  private LeadsCollector<kOut, vOut> collector = null;

  private Set<K> keys;
  private LeadsMapper<K, V, kOut, vOut> mapper = null;
  String site;
  private LeadsCombiner<?, ?> combiner;

  public LeadsMapperCallable(Cache<K, V> cache,
                             LeadsCollector<kOut, vOut> collector,
                             LeadsMapper<K, V, kOut, vOut> mapper, String site) {
    super("{}", collector.getCacheName());
    this.site = site;
    this.collector = collector;
    this.mapper = mapper;
  }

  @Override
  public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
    super.setEnvironment(cache, inputKeys);
//		this.cache =  cache;
//		this.keys = inputKeys;
//		collector.initializeCache(cache.getCacheManager());
  }

  @Override
  public void initialize() {
//    collector.initializeCache(inputCache.getCacheManager());
    super.initialize();
    collector.setOnMap(true);
    collector.setManager(this.embeddedCacheManager);
    collector.setEmanager(emanager);
    collector.setSite(site);
    collector.initializeCache(inputCache.getName(), imanager);
    if(combiner != null){
      combiner.initialize();
      collector.setCombiner((LeadsCombiner<kOut, vOut>) combiner);
      collector.setUseCombiner(true);
    }
    else{
      collector.setCombiner(null);
      collector.setUseCombiner(false);
    }
    mapper.initialize();
  }

//	public String call() throws Exception {
//
//		if (mapper == null) {
//			System.out.println(" Mapper not initialized ");
//		} else {
//         mapper.setCacheManager(cache.getCacheManager());
//			String result = imanager.getCacheManager().getAddress().toString();
//			final ClusteringDependentLogic cdl = cache.getAdvancedCache().getComponentRegistry().getComponent(ClusteringDependentLogic.class);
//			for(Object key : cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet()){
//				if(!cdl.localNodeIsPrimaryOwner(key))
//					continue;
//				V value = cache.get(key);
//				if (value != null) {
//					mapper.map((K)key, value, collector);
//				}
//			}
//
//			return result;
//		}
//		return null;
//	}

  @Override
  public void executeOn(K key, V value) {
    mapper.map(key, value, collector);
  }

  @Override
  public void finalizeCallable() {
    mapper.finalizeTask();
    System.err.println("Collector Finalize");
    collector.finalizeCollector();
    System.err.println("Super finalizeCallable");
    super.finalizeCallable();
    System.err.println("counter cache stop");
    collector.getCounterCache().stop();
  }

  public void setCombiner(LeadsCombiner<?, ?> combiner) {
    this.combiner = combiner;
  }

  public LeadsCombiner<?, ?> getCombiner() {
    return combiner;
  }
}
