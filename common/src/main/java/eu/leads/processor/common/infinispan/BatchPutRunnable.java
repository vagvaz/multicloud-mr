package eu.leads.processor.common.infinispan;

import eu.leads.processor.common.utils.PrintUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  }

  public TupleBuffer getBuffer(){
        return buffer;
    }
    public void setBuffer(TupleBuffer buffer){
        this.buffer = buffer;
    }
    @Override public void run() {
        boolean isok = false;
        try{

            while(retries > 0 && !isok){
                buffer.flushToMC();
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
          owner.addBatchPutRunnable(this);
        }else {
          EnsembleCacheUtils.addBatchPutRunnable(this);
        }
    }


}
