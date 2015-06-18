package eu.leads.processor.common.infinispan;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by vagvaz on 2/8/15.
 */
public class EnsembleInfinispanManager implements InfinispanManager {
  private Logger log = LoggerFactory.getLogger(this.getClass());
  private EnsembleCacheManager manager;
  private String configurationFile;
  private Configuration defaultConfig = null;
  private String connectionStrings;
  @Override public void setConfigurationFile(String configurationFile) {
    this.configurationFile  = configurationFile;
  }

  @Override public void startManager(String configurationFile) {

    //manager = new EnsembleCacheManager()
    manager = new EnsembleCacheManager(connectionStrings);
    manager.start();
  }

  @Override public EmbeddedCacheManager getCacheManager() {
    return null;
  }

  @Override public void stopManager() {
    manager.stop();

  }

  @Override public ConcurrentMap getPersisentCache(String name) {
    return manager.getCache(name);
  }

  @Override public ConcurrentMap getPersisentCache(String name, Configuration configuration) {
    return manager.getCache(name);
  }

  @Override
  public ConcurrentMap getIndexedPersistentCache(String name) {
    return null;
  }

  @Override
  public ConcurrentMap getIndexedPersistentCache(String name, Configuration configuration) {
    return null;
  }

  @Override public void removePersistentCache(String name) {

  }

  @Override public void addListener(Object listener, Cache cache) {

  }

  @Override public void addListener(Object listener, String name) {

  }

  @Override public void removeListener(Object listener, Cache cache) {

  }

  @Override public void removeListener(Object listener, String cacheNane) {

  }

  @Override public List<Address> getMembers() {
    return null;
  }

  @Override public Address getMemberName() {
    return  null;
  }

  @Override public boolean isStarted() {
    return true;
  }
}
