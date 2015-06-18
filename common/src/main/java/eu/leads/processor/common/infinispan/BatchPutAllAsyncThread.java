package eu.leads.processor.common.infinispan;

import eu.leads.processor.common.utils.PrintUtilities;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by vagvaz on 20/05/15.
 */
public class BatchPutAllAsyncThread extends Thread{

    private final Map<String, Map<Object, Object>> objects;
    private final Map<String, BasicCache> caches;
    private Map<NotifyingFuture, String> backup;
    private List<NotifyingFuture> futures;
    private Logger log;
    public BatchPutAllAsyncThread(Map<String, BasicCache> caches,
        Map<String, Map<Object, Object>> objects) {
        this.caches = caches;
        this.objects = objects;
        futures = new ArrayList<>();
        backup = new HashMap<>();
        log = LoggerFactory.getLogger(BatchPutAllAsyncThread.class);
    }

    @Override public void run() {
//        super.run();
        for(Map.Entry<String,Map<Object,Object>> entry : objects.entrySet()){
            BasicCache cache = caches.get(entry.getKey());
            NotifyingFuture nextFuture = cache.putAllAsync(entry.getValue());
            futures.add(nextFuture);
            backup.put(nextFuture,cache.getName());
        }
        List<NotifyingFuture> failed = null;
        while(!backup.isEmpty()) {
            failed = new ArrayList<>();
//            Iterator<Map.Entry<NotifyingFuture,String>> iterator = backup.entrySet().iterator();
            for(NotifyingFuture future : futures) {
                try {
                    future.get();
                    backup.remove(future);
                } catch (InterruptedException e) {
//                    e.printStackTrace();
                    log.error(e.getClass().toString());
                    PrintUtilities.logStackTrace(log,e.getStackTrace());
                   failed.add(future);
                } catch (ExecutionException e) {
//                    e.printStackTrace();
                    log.error(e.getClass().toString());
                    PrintUtilities.logStackTrace(log,e.getStackTrace());
                }

            }
            futures.clear();
            for(NotifyingFuture failedFuture : failed){
                //for the failed redo the action
                //Get Cache for that future
                log.error("EnsembleRetrying putting data to " + backup.get(failed));
                BasicCache cache = caches.get( backup.get(failedFuture) );
                //Get Map that we need to put
                Object ob = objects.get(cache.getName());
                //Remove old Future from Future backup
                backup.remove(failedFuture);

                //                    BasicCache cache = caches.get(backup.get(future));
                //Reddo operation
                NotifyingFuture nextFuture = cache.putAllAsync(objects.get(cache.getName()));
                futures.add(nextFuture);
                backup.put(nextFuture,cache.getName());
            }
        }
        caches.clear();
        objects.clear();
    }
}
