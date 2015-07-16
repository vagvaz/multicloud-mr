package eu.leads.processor.infinispan;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by vagvaz on 16/07/15.
 */
public class LocalIndexKeyIterator implements Iterator<Object> {
    String key;
    Long numberOfValues;
    Long currentCounter;
    Map<String,Object> dataCache;
    public LocalIndexKeyIterator(String key, Long counter,Map<String,Object> dataCache) {
        this.currentCounter = 0L;
        this.numberOfValues = counter;
        this.key = key;
        this.dataCache  = dataCache;
    }


    @Override public boolean hasNext() {
        if(currentCounter <= numberOfValues) {
            return true;
        }
        return false;
    }

    @Override public Object next() {
        if(currentCounter <= numberOfValues){
            Object result = dataCache.get(key+currentCounter);
            if(result != null){
                currentCounter++;
                return result;
            }
            throw new NoSuchElementException("LocalIndexIterator GOT NULL VALUE for  key " + key + " currentCounter " + currentCounter + " maximum " + numberOfValues);
        }
        throw new NoSuchElementException("LocalIndexIterator key " + key + " currentCounter " + currentCounter + " maximum " + numberOfValues);
    }

    @Override public void remove() {
        dataCache.remove(key+currentCounter);
    }
}
