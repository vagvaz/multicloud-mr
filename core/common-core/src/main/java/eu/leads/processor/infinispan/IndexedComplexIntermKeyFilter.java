package eu.leads.processor.infinispan;

import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;

import java.io.Serializable;

/**
 * Created by vagvaz on 4/7/15.
 */
public class IndexedComplexIntermKeyFilter implements KeyValueFilter,Serializable {
  String filterKey;
  public IndexedComplexIntermKeyFilter(String key) {
    this.filterKey = key;
  }

  @Override
  public boolean accept(Object key, Object value, Metadata metadata) {
    //      System.err.println("filter: " +key.toString() + " -> " + value.toString() + " with key " + filterKey);
//    if(! (value instanceof IndexedComplexIntermediateKey)){
//      if(value != null)
//        System.out.println(" class " + value.getClass().toString());
////      else
////        System.out.println("NULL");
//      return false;
//    }
    IndexedComplexIntermediateKey val = (IndexedComplexIntermediateKey)key;
    if(val.getKey().equals(filterKey)){
      //       System.err.println("ACCEPTING");
      return true;
    }
    return false;
  }
}
