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
      if(! (value instanceof IndexedComplexIntermediateKey)){
         return false;
      }
      IndexedComplexIntermediateKey val = (IndexedComplexIntermediateKey)value;
      if(val.getKey().equals(filterKey)){
  //       System.err.println("ACCEPTING");
         return true;
      }
      return false;
   }
}
