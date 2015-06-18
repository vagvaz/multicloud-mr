package eu.leads.processor.infinispan;

import eu.leads.processor.common.infinispan.InfinispanManager;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.context.Flag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by vagvaz on 3/7/15.
 */
public class LeadsIntermediateIterator<V> implements Iterator<V> {
  protected transient Cache intermediateDataCache;
  protected transient Cache indexSiteCache;
  private transient InfinispanManager imanager;
  private String key;
  private ComplexIntermediateKey baseIntermKey;
  private IndexedComplexIntermediateKey currentChunk;
  private Integer currentCounter = 0;
  private Iterator<IndexedComplexIntermediateKey> chunkIterator;
  private List<IndexedComplexIntermediateKey> list;
  private static Logger log = null;



  public LeadsIntermediateIterator(String key, String prefix, InfinispanManager imanager){
    log = LoggerFactory.getLogger(LeadsIntermediateIterator.class);
    this.imanager = imanager;
    intermediateDataCache =   (Cache) imanager.getPersisentCache(prefix + ".data");
//    intermediateDataCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);

    //createIndexCache for getting all the nodes that contain values with the same key! in a mc
    indexSiteCache = (Cache) imanager.getPersisentCache(prefix + ".indexed");
//    indexSiteCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);
    baseIntermKey = new ComplexIntermediateKey();
    baseIntermKey.setCounter(currentCounter);
    baseIntermKey.setKey(key);
    //    log.error("INDEXED SITE = " + indexSiteCache.size());
    //    for(Object keys : indexSiteCache.keySet()){
    //      log.error("key: " + keys.toString() + indexSiteCache.get(keys).toString());
    //    }
    // create query
    //    SearchManager sm = org.infinispan.query.Search.getSearchManager((Cache<?, ?>) indexSiteCache);
    //    QueryFactory qf = org.infinispan.query.Search.getQueryFactory((Cache<?, ?>) indexSiteCache); //Search.getQueryFactory((RemoteCache) indexSiteCache);
    //    org.infinispan.query.dsl.Query lucenequery = qf.from(IndexedComplexIntermediateKey.class)
    //                                                   .having("key").eq(key)
    //                                                   .toBuilder().build();
    //    ListIterator<Object> anIterator = lucenequery.list().listIterator();

    this.list = new ArrayList<>();
    try{
      CloseableIterable<Map.Entry<String, Object>> myIterable = ((Cache)indexSiteCache).getAdvancedCache().filterEntries(new IndexedComplexIntermKeyFilter(key));
      for (Map.Entry<String, Object> entry : myIterable) {
        //        System.err.println("ADDING TO LIST key: " + entry.getKey() + " value " + entry.getValue().toString());
        if(entry.getValue() instanceof  IndexedComplexIntermediateKey) {
          ComplexIntermediateKey c = new ComplexIntermediateKey((IndexedComplexIntermediateKey) entry.getValue());
          if (intermediateDataCache.containsKey(c)){
            list.add((IndexedComplexIntermediateKey) entry.getValue());
          }else{
            log.error("Indexed cache rejected because data Cache does not contain");
          }
        }
        else{
          log.error("\n\nGET [B once again");
          //          IndexedComplexIntermediateKey unserializedKey = new IndexedComplexIntermediateKey();
          //          unserializedKey.unserialize((byte[]) entry.getValue());
          //          System.err.println("ADDING [B] TO LIST key: " + entry.getKey() + " value " + unserializedKey.toString());
          //          list.add(unserializedKey);
        }
      }
    } catch (Exception e) {
      System.err.println("Exception on LeadsIntermediateIterator " + e.getClass().toString());
      System.err.println("Message: " + e.getMessage());
      log.error("Exception on LeadsIntermediateIterator " + e.getClass().toString());
      log.error("Message: " + e.getMessage());
    }

    //    this.list = new ArrayList<>();
    //    while(anIterator.hasNext()){
    //      Object o = anIterator.next();
    //      log.error("Adding to list " + o.toString());
    //      if(o instanceof IndexedComplexIntermediateKey) {
    //        IndexedComplexIntermediateKey ikey = (IndexedComplexIntermediateKey) o;
    //        list.add(ikey);
    //      }
    //      else{
    //        log.error("Error in indexSiteCache found class of type " + o.getClass().toString());
    //      }
    //
    //    }
    chunkIterator = list.iterator();

    if(chunkIterator.hasNext()) {
      currentChunk = chunkIterator.next();
      baseIntermKey = new ComplexIntermediateKey(currentChunk);
    }
    else{
      currentChunk = null;
    }

  }

  @Override public boolean hasNext() {
    boolean result = false;
    if(currentChunk == null || baseIntermKey == null)
      result = false;

    //    Object o = null;
    //    if(baseIntermKey != null)
    //     o = intermediateDataCache.get(new ComplexIntermediateKey(baseIntermKey));
    //    if(o != null){

    if (baseIntermKey != null && intermediateDataCache.containsKey(new ComplexIntermediateKey(baseIntermKey))) {
      //      System.err.println("baseIntermKey " + baseIntermKey);
      //      System.err.println("with object " + o.toString());
      result = true;
    }

    if(chunkIterator != null && chunkIterator.hasNext()) {
      //      System.err.println("chunk has next " + chunkIterator.toString());
      //      PrintUtilities.printList(list);
      result = true;
    }

    //    System.err.println("leadsIntermediateIterator hasNext returns " + result);
    return result;
  }

  @Override public V next() {
    //    if(!hasNext()){
    //      throw new NoSuchElementException("LeadsIntermediateIterator the iterator does not have next");
    //    }
    //    System.err.println("in next ");
    //    log.error(baseIntermKey.toString());
    if(baseIntermKey == null || chunkIterator == null){
      throw new NoSuchElementException("LeadIntermediateIterator no more Elements");
      //return null;
    }
    V returnValue = (V) intermediateDataCache.get(new ComplexIntermediateKey(baseIntermKey));
    if(returnValue == null){

      if(chunkIterator.hasNext()) {
        currentChunk = chunkIterator.next();
        baseIntermKey = new ComplexIntermediateKey(currentChunk);
        return next();
      }
      else{
        baseIntermKey = null;
        currentChunk = null;
        throw new NoSuchElementException("LeadIntermediateIterator no more Elements");
      }
      //      System.err.println("\n\n\nERROR NULL GET FROM intermediate data cache " + baseIntermKey.toString() + "\n cache size = " + intermediateDataCache.size());
      //      throw new NoSuchElementException("LeadsIntermediateIterator read from cache returned NULL");
    }
    else {
      baseIntermKey.next();
    }
    //    baseIntermKey = baseIntermKey.next();
    //    Object o = intermediateDataCache.get(baseIntermKey);
    //    if(o == null){
    //
    //    if(!intermediateDataCache.containsKey(new ComplexIntermediateKey(baseIntermKey))){
    //      if(chunkIterator.hasNext()) {
    //        currentChunk = chunkIterator.next();
    //        baseIntermKey = new ComplexIntermediateKey(currentChunk);
    //      }
    //      else{
    //        baseIntermKey = null;
    //        currentChunk = null;
    //      }
    //
    //
    //    }
    if(returnValue != null) {
      //      System.err.println("out next ");
      return returnValue;
    }
    else {
      //      if(chunkIterator.hasNext()) {
      //        System.err.println("TbaseInterm Key is not contained and chunkIterator has NeXt");
      //        System.err.println(this.toString());
      //      }
      //      currentChunk = null;
      throw new NoSuchElementException("LeadsIntermediateIterator return Value NULL at the end");
    }
  }

  @Override public void remove() {

  }

  @Override public String toString() {
    String result =  "LeadsIntermediateIterator{" +
        "intermediateDataCache=" + intermediateDataCache.getName() +
        ", indexSiteCache=" + indexSiteCache.getName() +
        ", imanager=" + imanager.getCacheManager().getAddress().toString();
    //    System.err.println(result);
    String resultb =
        ", baseIntermKey=" + baseIntermKey.toString();
    result += resultb;
    //    System.err.println(resultb);

    String resultc =
        ", list=" + list.size() +
            ", currentCounter=" + currentCounter.toString() +

            '}';
    //    PrintUtilities.printList(list);
    result += resultc;
    //    System.err.println(resultc);
    result += ", currentChunk=" + currentChunk;
    //    System.err.println(result);
    return result;
  }
}
