package eu.leads.processor.common.infinispan;

import org.infinispan.Cache;
import org.infinispan.distexec.DistributedCallable;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by vagvaz on 6/6/14.
 */
public class RemoveListenerCallable<K, V> implements DistributedCallable<K, V, Void> {
    private static final long serialVersionUID = 8331682008912636730L;
    private final String cacheName;
    private final Object listener;
    //    private final Configuration configuration;
    private transient Cache<K, V> cache;

    public RemoveListenerCallable(String cacheName, Object listener) {
        this.cacheName = cacheName;
        this.listener = listener;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Void call() throws Exception {
        //Initially try to remove using comparable interface
        Set<Object> listeners = this.cache.getListeners();
        Iterator<Object> iterator = listeners.iterator();
        while (iterator.hasNext()) {
            Object current = iterator.next();
            if (this.listener.equals(current)) {
                cache.removeListener(current);
                return null;
            }
        }
        //Try to remove by class Name
        iterator = listeners.iterator();
        while (iterator.hasNext()) {
            Object current = iterator.next();
            if (this.listener.getClass().equals(current.getClass())) {
                cache.removeListener(current);
                return null;
            }
        }
        //no listener to remove from this cache
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
        this.cache = cache;
    }
}
