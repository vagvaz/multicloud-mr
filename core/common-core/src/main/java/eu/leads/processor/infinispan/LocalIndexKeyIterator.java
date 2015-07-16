package eu.leads.processor.infinispan;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by vagvaz on 16/07/15.
 */
public class LocalIndexKeyIterator implements Iterator<Object> {
    String key;
    Integer numberOfValues;
    Integer currentCounter;
    Map<String,Object> dataCache;
    public LocalIndexKeyIterator(String key, Integer counter,Map<String,Object> dataCache) {
        this.currentCounter = 0;
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
            //            if(result != null){
            currentCounter++;
            return result;
            //            }
            //            throw new NoSuchElementException("LocalIndexIterator GOT NULL VALUE for  key " + key + " currentCounter " + currentCounter + " maximum " + numberOfValues);
        }
        throw new NoSuchElementException("LocalIndexIterator key " + key + " currentCounter " + currentCounter + " maximum " + numberOfValues);
    }

    @Override public void remove() {
        System.err.println("SHOULD NOT BE USED");
//        if(currentCounter > 0) {
//            dataCache.remove(key + (currentCounter - 1));
//        }
    }
}
