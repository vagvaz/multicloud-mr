package eu.leads.processor.core.netty;

import eu.leads.processor.common.infinispan.BatchPutRunnable;
import eu.leads.processor.common.infinispan.KeyValueDataTransfer;
import eu.leads.processor.common.infinispan.TupleBuffer;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import io.netty.channel.Channel;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.Site;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by vagvaz on 10/21/15.
 * This class will act as a proxy to NettyDataTransport.
 * NettyKeyValueDataTransfer provides the minimum required interface and acts as replacement for the
 * EnsembleCacheUtilsSingle, which is used for put/transferring data from one stage (map,reducelocal,reduce) to the next one
 * This class is not going to send directly data to other nodes, but it will use the NettyDataTransport class for sending data.
 * However, the class will have all the information required to decide which node must receive these data.
 * This class also will gather the necessary metrics (volume of remote/local data written)
 * The local Microcloud can be found from LQPConfiguration.getInstance() and the local IP also can be resolved look at the
 * resolveMC of EnsembleCacheUtils
 */
public class NettyKeyValueDataTransfer implements KeyValueDataTransfer {
  /**
   * This method is not used from anywhere in the project
   *
   * @throws ExecutionException
   * @throws InterruptedException
   */

  private List<String> nodes;
  private Map<String,Map<String,TupleBuffer>> nodeMaps;
  private Logger log;
  private EnsembleCacheManager manager;
  private int batchSize;

  @Override public void clean() throws ExecutionException, InterruptedException {

  }

  /**
   * Initializes the internal structures of the class.
   * From the EnsembleCacheManager the number of microclouds and the individual IPs can be retrieved. Using the method
   * manager.sites().
   * Using the site.getName() you get all the ips for a microcloud separated by ; as in json configuration componentAddrs
   * This
   *
   * @param manager
   */
  @Override public void initialize(EnsembleCacheManager manager) {
    initialize(manager,true);
  }

  /**
   * This method will not be used anywhere in this project. It is used for initializing the data structures without starting an
   * Infinspan CacheManager
   *
   * @param manager
   * @param isEmbedded
   */
  @Override public void initialize(EnsembleCacheManager manager, boolean isEmbedded) {
    NettyDataTransport.initializeNodes();
    log = LoggerFactory.getLogger(this.getClass());
    batchSize = LQPConfiguration.getInstance().getConfiguration().getInt("node.ensemble.batchsize", 100);
    this.manager = manager;
    TreeMap map = (TreeMap) NettyDataTransport.getNodes();

    nodes = new ArrayList<>(new TreeSet(map.descendingKeySet()));
    nodeMaps = new HashMap<>();
    for(String nodeId : nodes){
//
//        nodes.add(nodeId);
        nodeMaps.put(nodeId, new HashMap<String, TupleBuffer>());
      }
//    }


  }

  /**
   * As auxiliary puts we have defined when we used ensembleCacheUtilsSingle class to a single datum into a cache.
   * This in our last approach spawned a thread and executed the operation. So in order not to lose data we needed to wait for
   * the completion of such operations. Depending on the design of the data transfer we might need this method.
   * So this method should block until all simple cache.put(key,value) operations are completed. ( Yes we dont have the notion of cache)
   *
   * @throws InterruptedException
   */
  @Override public void waitForAuxPuts() throws InterruptedException {
    try {
      waitForAllPuts();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  /**
   * This method should block until all data transfer operations are completed.
   *
   * @throws InterruptedException
   * @throws ExecutionException
   */
  @Override public void waitForAllPuts() throws InterruptedException, ExecutionException {
    for(Map.Entry<String,Map<String,TupleBuffer>> nodeEntry : nodeMaps.entrySet()){
      Map<String,TupleBuffer> nodeBuffer = nodeEntry.getValue();
      String targetNode = nodeEntry.getKey();

      for(Map.Entry<String,TupleBuffer> cacheEntry : nodeBuffer.entrySet()){
        String cacheName = cacheEntry.getKey();
        TupleBuffer buffer = cacheEntry.getValue();
        if(buffer.getBuffer().size() > 0) {
          NettyDataTransport.send(targetNode, cacheName, buffer.serialize());
        }
      }
    }
    NettyDataTransport.waitEverything();
  }

  /**
   * putToCache is used to put a key,value pair into an infinispan cache.
   * We are going to use cache.getName() to inform the receiver where (In which Index) to put the key,value pair
   * PseudoCode
   * String indexName = cache.getName()
   * int index = key.hashCode()
   * Node node = nodes.get(MAth.abs(index))
   * NettyDataTransport.send(node,indexName,key,value);
   *
   * @param cache
   * @param key
   * @param value
   */
  @Override public void putToCache(BasicCache cache, Object key, Object value) {
    int index = Math.abs(key.hashCode() ) % nodes.size();
    String target = nodes.get(index);
    Map<String,TupleBuffer> nodeMap = nodeMaps.get(target);
    if(nodeMap == null){
      PrintUtilities.printAndLog(log,"node has not been initialized ");
      NettyDataTransport.send(target,cache.getName(),key,value);
      return;
    }
    TupleBuffer buffer = nodeMap.get(cache.getName());
    if(buffer == null){
      buffer = new TupleBuffer(batchSize,cache.getName(),manager, LQPConfiguration.getInstance().getMicroClusterName(),this);
      nodeMap.put(cache.getName(),buffer);
    }
    if(buffer.add(key,value)){
      byte[] bytes = buffer.serialize();
      NettyDataTransport.send(target,buffer.getCacheName(),bytes);
    }
  }

  /**
   * In LEADS putToCache is implemented in a more complex way to do some batching if the key should be transferred to
   * another microcloud and so on. So when we needed a direct put to the cache we used this method.
   *
   * @param cache
   * @param key
   * @param value
   */
  @Override public void putToCacheDirect(BasicCache cache, Object key, Object value) {
    putToCache(cache, key, value);
  }

  /**
   * This Method should write how many bytes where written to a remote microcloud and how many to the local.
   */
  @Override public void spillMetricData() {
    NettyDataTransport.spillMetricData();
  }

  /**
   * This method will not be used.
   *
   * @param bpr
   */
  @Override public void submit(BatchPutRunnable bpr) {

  }

  /**
   * Also this method will not be used.
   *
   * @param length
   */
  @Override public void updateRemoteBytes(int length) {

  }

}
