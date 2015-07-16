package eu.leads.processor.infinispan;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.infinispan.IntermediateKeyIndex;
import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;

import java.io.FileWriter;
import java.util.Map;

/**
 * Created by vagvaz on 16/07/15.
 */
@Listener(sync = true,primaryOnly = true,clustered = false)
public class LocalIndexListener {

    String cacheName;
    IntermediateKeyIndex index;
    Cache keysCache;
    Cache dataCache;
    public LocalIndexListener(InfinispanManager manager, String cacheName) {
        this.cacheName = cacheName;
        this.keysCache = manager.getLocalCache(cacheName+".index.keys");
        this.dataCache = manager.getLocalCache(cacheName+".index.data");
        this.index = new IntermediateKeyIndex(keysCache,dataCache);
    }

}
