package eu.leads.processor.infinispan;

import org.infinispan.Cache;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by vagvaz on 16/07/15.
 */
public class IntermediateKeyIndex {
    private Map<String,Integer> keysCache;
    private Map<String,Object> dataCache;

    public IntermediateKeyIndex(Cache keysCache, Cache dataCache) {
        this.keysCache = keysCache;
        this.dataCache = dataCache;
    }

    public void put(String key, Object value){
        Integer count = keysCache.get(key);
        if(count == null){
            count = 0;
        }
        else{
            count++;
        }
        dataCache.put(key+count.toString(),value);
        keysCache.put(key,count);
    }

    public Set<Map.Entry<String,Integer>> getKeysIterator(){
        return keysCache.entrySet();
    }

    public Iterator<Object> getKeyIterator(String key, Integer counter){
        return new LocalIndexKeyIterator(key,counter,dataCache);
    }


}
