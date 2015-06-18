package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskBuilder;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by vagvaz on 9/22/14.
 */
public class ScanOperator extends BasicOperator {

   public ScanOperator(Node com, InfinispanManager persistence,LogProxy log, Action action) {
      super(com,persistence,log,action);
   }

   //  public FilterOperator(PlanNode node) {
   //      super(node, OperatorType.FILTER);
   //  }




//   @Override
   public void run2() {

      System.err.println("RUNNNING SCAN OPERATOR");
      inputCache = (Cache) manager.getPersisentCache(getInput());

      outputCache = (Cache)manager.getPersisentCache(getOutput());

      if(inputCache.size() == 0){
         cleanup();
         return;
      }
      DistributedExecutorService des = new DefaultExecutorService(inputCache);
      ScanCallableUpdate<String,Tuple> callable = new ScanCallableUpdate<>(conf.toString(),getOutput());
//      ScanCallable callable = new ScanCallable(conf.toString(),getOutput());
      DistributedTaskBuilder builder = des.createDistributedTaskBuilder( callable);
      builder.timeout(1, TimeUnit.HOURS);
      DistributedTask task = builder.build();
      List<Future<String>> res = des.submitEverywhere(task);
//      Future<String> res = des.submit(callable);
      List<String> addresses = new ArrayList<String>();
      try {
         if (res != null) {
            for (Future<?> result : res) {
               System.out.println(result.get());
               addresses.add((String) result.get());
            }
            System.out.println("mapper Execution is done");
         }
         else
         {
            System.out.println("mapper Execution not done");
         }
      } catch (InterruptedException e) {
         e.printStackTrace();
      } catch (ExecutionException e) {
         e.printStackTrace();
      }
      System.err.println("FINISHED RUNNING SCAN " + outputCache.size());
      updateStatistics(inputCache,null,outputCache);
      cleanup();
   }

   @Override
   public void init(JsonObject config) {
      inputCache = (Cache) manager.getPersisentCache(getInput());
   }

   @Override
   public void execute() {
      super.execute();
   }

   @Override
   public void cleanup() {

      System.err.println("CLEANING UP " );
      super.cleanup();
   }

   @Override
   public void createCaches(boolean isRemote, boolean executeOnlyMap, boolean executeOnlyReduce) {
      Set<String> targetMC = getTargetMC();
      for(String mc : targetMC){
         createCache(mc,getOutput());
      }
   }

   @Override
   public void setupMapCallable() {
      inputCache = (Cache) manager.getPersisentCache(getInput());
      mapperCallable = new ScanCallableUpdate<>(conf.toString(),getOutput());
   }

   @Override
   public void setupReduceCallable() {

   }

   @Override
   public boolean isSingleStage() {
      return true;
   }


}
