package eu.leads.processor.infinispan.operators;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.infinispan.LeadsMapper;
import eu.leads.processor.infinispan.operators.mapreduce.ProjectMapper;
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



@JsonAutoDetect
public class ProjectOperator extends BasicOperator {

   public ProjectOperator(Node com, InfinispanManager persistence,LogProxy log, Action actionData) {
      super(com, persistence,log, actionData);

   }

   @Override
    public void init(JsonObject config) {
      super.init(conf);
      conf.putString("output", getOutput());
     inputCache = (Cache) manager.getPersisentCache(getInput());

     //      LeadsMapper projectMapper = new ProjectMapper(conf.toString());
//      setMapper(projectMapper);
//      setReducer(null);
//      init_statistics(this.getClass().getCanonicalName());
    }

    @Override
    public void execute() {
      super.execute();
//      inputCache = (Cache) manager.getPersisentCache(getInput());
//
//      Cache outputCache = (Cache)manager.getPersisentCache(getOutput());
//
//      if(inputCache.size() == 0){
//        cleanup();
//        return;
//      }
//      DistributedExecutorService des = new DefaultExecutorService(inputCache);
//      ProjectCallableUpdated<String,Tuple> callable = new ProjectCallableUpdated<>(conf.toString(),getOutput
//                                                                                                    ());
//      DistributedTaskBuilder builder = des.createDistributedTaskBuilder( callable);
//      builder.timeout(1, TimeUnit.HOURS);
//      DistributedTask task = builder.build();
//      List<Future<String>> res = des.submitEverywhere(task);
//      //      Future<String> res = des.submit(callable);
//      List<String> addresses = new ArrayList<String>();
//      try {
//        if (res != null) {
//          for (Future<?> result : res) {
//            System.out.println(result.get());
//            addresses.add((String) result.get());
//          }
//          System.out.println("mapper Execution is done");
//        }
//        else
//        {
//          System.out.println("mapper Execution not done");
//        }
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      } catch (ExecutionException e) {
//        e.printStackTrace();
//      }
//      System.err.println("FINISHED RUNNING SCAN " + outputCache.size());
//      updateStatistics(inputCache,null,outputCache);
//      cleanup();

    }

    @Override
    public void cleanup() {
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
     System.out.println("INPUT CACHE SIZE " + inputCache.size());
      mapperCallable =  new ProjectCallableUpdated<>(conf.toString(),getOutput());
   }

   @Override
   public void setupReduceCallable() {

   }

   @Override
   public boolean isSingleStage() {
      return true;
   }
}
