package eu.leads.processor.common.infinispan;

import eu.leads.processor.common.LeadsListener;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.FutureListener;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.context.Flag;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * Created by vagvaz on 8/30/15.
 */
@Listener(sync = true,primaryOnly = true,clustered = false)
public class BatchPutListener implements LeadsListener {
    private transient InfinispanManager manager;
    private String compressedCache;
    private String targetCacheName;
    private transient Cache targetCache;
    private transient ConcurrentMap<NotifyingFuture<Void>,NotifyingFuture<Void>> futures;
    private transient Object mutex = null;
    private transient  EnsembleCacheUtilsSingle ensembleCacheUtilsSingle;
    private transient Logger log = LoggerFactory.getLogger(BatchPutListener.class);

    public BatchPutListener(String compressedCache,String targetCacheName){
        this.compressedCache = compressedCache;
        this.targetCacheName = targetCacheName;
    }
    @Override public InfinispanManager getManager() {
        return manager;
    }

    @Override public void setManager(InfinispanManager manager) {
        this.manager = manager;
    }

    @Override public void initialize(InfinispanManager manager, JsonObject conf) {
        this.manager = manager;
        mutex =  new Object();
        if(conf != null) {
            if (conf.containsField("target")) {
                targetCacheName = conf.getString("target");
            }
        }
        System.err.println("get target cache");
        targetCache = (Cache) manager.getPersisentCache(targetCacheName);
        futures = new ConcurrentHashMap<>();
        System.err.println("init ensembleCacheUtilsSingle");
        ensembleCacheUtilsSingle  = new EnsembleCacheUtilsSingle();
        System.err.println("end");
    }

    @Override public void initialize(InfinispanManager manager) {
        this.initialize(manager,null);
    }

    @Override public String getId() {
        return BatchPutListener.class.toString();
    }

    @Override public void close() {
        futures.clear();
        targetCache = null;
        manager = null;
    }

    @Override public void setConfString(String s) {

    }

    @CacheEntryCreated
    public void created(CacheEntryCreatedEvent event){
        if(event.isPre()){
            return;
        }
        batchPut(event.getKey(),event.getValue());
    }

    private void batchPut(Object key, Object value) {
        //        System.out.println("RUN BatchPut");
        try {
            byte[] b = (byte[]) value;
            if (b.length == 1 && b[0] == -1) {
                waitForPendingPuts();
                return;
            }
            TupleBuffer tupleBuffer = new TupleBuffer((byte[]) value);
//            Map tmpb = new HashMap();
            for (Map.Entry<Object, Object> entry : tupleBuffer.getBuffer().entrySet()) {
                ensembleCacheUtilsSingle.putToCacheDirect(targetCache,entry.getKey(),entry.getValue());
            }
//            tupleBuffer.flushToCache(targetCache);
//            NotifyingFuture f = tupleBuffer.flushToCache(targetCache);
//            futures.put(f,f);
            tupleBuffer.getBuffer().clear();
            tupleBuffer = null;

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @CacheEntryModified
    public void modified(CacheEntryModifiedEvent event) {
        if(event.isPre()){
            return;
        }
        batchPut(event.getKey(),event.getValue());
    }

    public void waitForPendingPuts(){
        if(futures == null)
            return;

        boolean isok = false;
        boolean retry = false;
        while(!isok){

            try {
                ensembleCacheUtilsSingle.waitForAuxPuts();
            } catch (Exception e) {
                PrintUtilities.logStackTrace(log,e.getStackTrace());
            }
            try{
                for(Map.Entry<NotifyingFuture<Void>,NotifyingFuture<Void>> entry : futures.entrySet()){
                    entry.getKey().get();
                }
                if(retry){

                }
                isok = true;
            }
            catch (Exception e){
                System.err.println("Exception " +  e.getClass().toString() + " in BatchPUtListener waitForPendingPuts" + e.getMessage());
                e.printStackTrace();
                PrintUtilities.logStackTrace(log, e.getStackTrace());
                retry = true;
                isok = true;
            }
        }
    }
}
