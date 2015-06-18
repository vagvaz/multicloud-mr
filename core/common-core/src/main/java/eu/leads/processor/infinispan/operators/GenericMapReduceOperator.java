package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.infinispan.*;
import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskBuilder;
import org.vertx.java.core.json.JsonObject;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by vagvaz on 3/31/15.
 */
public class GenericMapReduceOperator extends MapReduceOperator {
   public GenericMapReduceOperator(Node com, InfinispanManager persistence, LogProxy log, Action action) {
      super(com, persistence, log, action);
   }

   @Override
   public void init(JsonObject config) {
      super.init(config);
      //Read Mapper class
      //read mapjar path
      //read reducer class
      //read reducer path
      //read Config path or //read config
      //Storage //storagetype
      //tmpdir prefix

   }

   @Override
   public void run() {
      long startTime = System.nanoTime();
      if(reducer == null)
         reducer = new LeadsReducer("");
      System.out.println("RUN MR on " + inputCache.getName());
      //       MapReduceTask<String,String,String,String> task = new MapReduceTask(inputCache);
      //               .reducedWith((org.infinispan.distexec.mapreduce.Reducer<String, String>) reducer);
      //       task.timeout(1, TimeUnit.HOURS);
      //       task.execute();

      DistributedExecutorService des = new DefaultExecutorService((Cache<?, ?>) inputCache);

      GenericMapperCallable mapperCallable = new GenericMapperCallable(conf.toString(),intermediateCacheName);
//                                                   ((Cache) inputCache,collector,mapper,
//                                                                          LQPConfiguration.getInstance().getMicroClusterName());
      DistributedTaskBuilder builder =des.createDistributedTaskBuilder(mapperCallable);
      builder.timeout(1, TimeUnit.HOURS);
      DistributedTask task = builder.build();
      List<Future<?>> res = des.submitEverywhere(task);
      try {
         if (res != null) {
            for (Future<?> result : res) {
               result.get();
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
      System.err.println("keysCache " + keysCache.size());
      System.err.println("dataCache " + intermediateDataCache.size());
      System.err.println("indexedCache " + indexSiteCache.size());
      if(reducer != null) {

         GenericReducerCallable reducerCacllable = new GenericReducerCallable(conf.toString(),getOutput());
//                                                         (outputCache.getName(), reducer,
//                                                                                 intermediateCacheName);
         DistributedExecutorService des_inter = new DefaultExecutorService((Cache<?, ?>) keysCache);
         DistributedTaskBuilder reduceTaskBuilder = des_inter.createDistributedTaskBuilder(reducerCacllable);
         reduceTaskBuilder.timeout(1,TimeUnit.HOURS);
         DistributedTask reduceTask = reduceTaskBuilder.build();
         List<Future<?>> reducers_res= des_inter
                                               .submitEverywhere(reduceTask);
         try {
            if (reducers_res != null) {
               for (Future<?> result : reducers_res) {
                  System.err.println("wait " + System.currentTimeMillis());
                  System.err.println(result.get());
                  System.err.println("wait end" + System.currentTimeMillis());
               }
               System.out.println("reducer Execution is done");
            } else {
               System.out.println("reducer Execution not done");
            }
         } catch (InterruptedException e) {
            e.printStackTrace();
         } catch (ExecutionException e) {
            e.printStackTrace();
         }
      }
      //Store Values for statistics
      updateStatistics(inputCache,null,outputCache);
      cleanup();
   }
}
