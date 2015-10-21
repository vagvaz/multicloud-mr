package eu.leads.processor.common.infinispan;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.ensemble.EnsembleCacheManager;

import java.util.concurrent.ExecutionException;

/**
 * Created by vagvaz on 10/21/15.
 */
public interface KeyValueDataTransfer {
  void clean() throws ExecutionException, InterruptedException;

  void initialize(EnsembleCacheManager manager);

  void initialize(EnsembleCacheManager manager, boolean isEmbedded);


  void waitForAuxPuts() throws InterruptedException;

  void waitForAllPuts() throws InterruptedException, ExecutionException;


  void putToCache(BasicCache cache, Object key, Object value);

  void putToCacheDirect(BasicCache cache, Object key, Object value);

  void spillMetricData();

  void submit(BatchPutRunnable bpr);

  void updateRemoteBytes(int length);
}
