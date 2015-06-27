package eu.leads.processor.common.test;

import eu.leads.processor.common.infinispan.CacheManagerFactory;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.infinispan.ComplexIntermediateKey;
import eu.leads.processor.infinispan.IndexedComplexIntermediateKey;
import eu.leads.processor.infinispan.LeadsIntermediateIterator;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.api.BasicCache;

import java.util.NoSuchElementException;

/**
 * Created by vagvaz on 3/27/15.
 */
public class IntermediateResultsTest {

  static BasicCache indexedCache;
  static BasicCache dataCache;
  static BasicCache keysCache;
  static String[] nodes= {"node0","node1","node2","node3","node00"};//,"node11","node22","node33"};
  static String[] microClouds = {"mc0"};//,"mc1"};//,"mc2","mc01","mc11","mc21"};
  static String[] keys;
  static String cacheName = "acache";
  static int numOfkeys = 2;
  static int numOfNodes = 1;
  static int numOfMicroClouds = 500; //500
  static int valuesPerKey = 2;
  static RemoteCacheManager rmanager;
  public static void main(String[] args) {
    LQPConfiguration.initialize();
    rmanager = createRemoteCacheManager();
    InfinispanManager manager = InfinispanClusterSingleton.getInstance().getManager();
    //    InfinispanManager manager11 = CacheManagerFactory.createCacheManager();
    //    InfinispanManager manager12 = CacheManagerFactory.createCacheManager();
    //    InfinispanManager manager13 = CacheManagerFactory.createCacheManager();
    //    InfinispanManager manager14 = CacheManagerFactory.createCacheManager();
    // create required caches
    indexedCache = (BasicCache) manager.getPersisentCache("prefix.indexed");
    dataCache = (BasicCache)manager.getPersisentCache("prefix.data");
    keysCache = (BasicCache)manager.getPersisentCache("keysCache");

    //get remote caches in order to emulate ensemble!
    indexedCache = rmanager.getCache("prefix.indexed");
    dataCache = rmanager.getCache("prefix.data");
    keysCache = rmanager.getCache("keysCache");

    //generate keys
    keys = new String[numOfkeys];
    for (int index = 0; index < numOfkeys; index++) {
      keys[index] = "key"+index;
      keysCache.put(keys[index],keys[index]);
    }

    //generate nodes and micro clouds
    nodes = new String[numOfNodes];
    microClouds = new String[numOfMicroClouds];

    for (int i = 0; i < numOfMicroClouds; i++) {
      microClouds[i] = "mc"+i;
    }

    for(int i = 0; i < numOfNodes;i++){
      nodes[i] = "node"+i;
    }
    //generate intermediate keyValuePairs
    generateIntermKeyValue(keysCache, dataCache, indexedCache, valuesPerKey, keys, nodes,
        microClouds);

    int counter = 0;
    System.err.println("Start Iteration");
    for(String k : keys){
      int keyCounter = 0;
      //initialize iterator
      LeadsIntermediateIterator iterator = new LeadsIntermediateIterator(k,"prefix",InfinispanClusterSingleton.getInstance().getManager(),numOfMicroClouds*numOfNodes);

      while(true){
        try {
          //          System.out.println(keyCounter + ": " + iterator.next().toString());
          iterator.next();
          keyCounter++;
          counter++;
          if(keyCounter % 10 == 0)
            System.out.print(".");
        }catch(Exception e ){
          if ( e instanceof NoSuchElementException){
            //            System.err.println("End of Iteration");
            break;
          }
          e.printStackTrace();
        }
      }
      //check if key iterated correctly
      if(keyCounter != (valuesPerKey*numOfMicroClouds*numOfNodes))
      {
        System.err.println("key " + k + " iteration size: " + keyCounter + " instead of " + valuesPerKey*numOfMicroClouds*numOfNodes + " number of missed iterations" + (valuesPerKey*numOfMicroClouds*numOfNodes -keyCounter)/valuesPerKey );
      }
    }
    System.err.println("Total counted " + counter + " total " + keys.length* microClouds.length*nodes.length*valuesPerKey);
    System.exit(0);
  }

  private static void generateIntermKeyValue(BasicCache keysCache, BasicCache dataCache,
      BasicCache indexedCache, int valuesPerKey, String[] keys,
      String[] nodes, String[] microClouds) {
    //    IndexedComplexIntermediateKey indexedKey = new IndexedComplexIntermediateKey();
    //    ComplexIntermediateKey ikey = new ComplexIntermediateKey();
    for(String node : nodes){
      for(String site : microClouds) {
        for (String key : keys) {
          keysCache.put(key, key);
          IndexedComplexIntermediateKey indexedKey = new IndexedComplexIntermediateKey();
          indexedKey.setKey(key);
          indexedKey.setNode(node);
          indexedKey.setSite(site);
          indexedKey.setCache(cacheName);
          for (int counter = 0; counter < valuesPerKey; counter++) {
            ComplexIntermediateKey ikey = new ComplexIntermediateKey();
            ikey.setCounter(counter);
            ikey.setKey(key);
            ikey.setSite(site);
            ikey.setNode(node);
            ikey.setCache(cacheName);
            indexedCache.put(indexedKey, new IndexedComplexIntermediateKey(indexedKey));
            dataCache.put(ikey, new ComplexIntermediateKey(ikey));
          }
          System.out.println("Generated " + node + " " + site + " " + key);
        }
      }
    }

  }

  private static RemoteCacheManager createRemoteCacheManager() {
    ConfigurationBuilder builder = new ConfigurationBuilder();
    builder.addServer().host(LQPConfiguration.getConf().getString("node.ip")).port(11222);
    return new RemoteCacheManager(builder.build());
  }

}
