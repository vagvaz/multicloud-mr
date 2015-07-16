package eu.leads.processor.infinispan;

import org.infinispan.Cache;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by vagvaz on 16/07/15.
 */
public class IntermediateKeyIndex {
    private Map<String,Long> keysCache;
    private Map<String,Object> dataCache;

    public IntermediateKeyIndex(Cache keysCache, Cache dataCache) {
        this.keysCache = keysCache;
        this.dataCache = dataCache;
    }

    public void put(String key, Object value){
        Long count = keysCache.get(key);
        if(count == null){
            count = 0L;
        }
        else{
            count++;
        }
        dataCache.put(key+count.toString(),value);
        keysCache.put(key,count);
    }

    public Set<Map.Entry<String,Long>> getKeysIterator(){
        return keysCache.entrySet();
    }

    public Iterator<Object> getKeyIterator(String key, Long counter){
        return new LocalIndexKeyIterator(key,counter,dataCache);
    }


}
