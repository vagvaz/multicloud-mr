package eu.leads.processor.common.infinispan;

import eu.leads.processor.common.utils.PrintUtilities;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by vagvaz on 9/1/15.
 */
public class BatchPutRunnable implements Runnable{
  private EnsembleCacheUtilsSingle owner;
  TupleBuffer buffer = null;
    int retries = 10;
    Logger log = LoggerFactory.getLogger(BatchPutRunnable.class);

    public BatchPutRunnable(){

    }
    public BatchPutRunnable(int retries){
        this.retries=retries ;
    }

  public BatchPutRunnable(int i, EnsembleCacheUtilsSingle ensembleCacheUtilsSingle) {
    this.owner = ensembleCacheUtilsSingle;
    this.retries = i;
  }

  public TupleBuffer getBuffer(){
        return buffer;
    }
    public void setBuffer(TupleBuffer buffer){
        this.buffer = buffer;
    }
    @Override public void run() {
        boolean isok = false;
      Map data = buffer.flushToMC();
      if(data==null || data.size() == 0){
        if(owner != null){
//          System.err.println("ADDING batchput to single");
          owner.addBatchPutRunnable(this);
        }else {
//          System.err.println("ADDING batchput to static");
          EnsembleCacheUtils.addBatchPutRunnable(this);
        }
      }
      long counter = (long) data.get("counter");
      EnsembleCache cache = (EnsembleCache) data.get("cache");
      String uuid = (String) data.get("uuid");
      byte[] bytes = (byte[]) data.get("bytes");
        try{

//            EnsembleCacheUtilsSingle ensembleCacheUtilsSingle = (EnsembleCacheUtilsSingle) data.get("ensemble");

            while(retries > 0 && !isok){
                cache.put(uuid + ":" + Long.toString(counter),bytes);
                isok = true;
            }
        }
        catch (Exception e){
            System.err.println(
                "Exception in BatchPutRunnbale= " + e.getClass().toString() + " " + e.getMessage());
            e.printStackTrace();
            retries--;
            PrintUtilities.logStackTrace(log,e.getStackTrace());
        }
        if(owner != null){
//          System.err.println("ADDING batchput to single");
          owner.addBatchPutRunnable(this);
        }else {
//          System.err.println("ADDING batchput to static");
          EnsembleCacheUtils.addBatchPutRunnable(this);
        }
    }


}
