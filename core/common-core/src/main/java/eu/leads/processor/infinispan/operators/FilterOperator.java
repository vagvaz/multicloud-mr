package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.math.FilterOperatorTree;
import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskBuilder;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 7:06 AM
 * To change this template use File | Settings | File Templates.
 */
//Filter Operator
public class FilterOperator extends BasicOperator {


    private FilterOperatorTree tree;


    public FilterOperator(Node com, InfinispanManager persistence,LogProxy log, Action action) {

       super(com, persistence,log, action);
       JsonElement qual = conf.getObject("body").getElement("qual");
       tree = new FilterOperatorTree(qual);
        inputCache = (Cache) manager.getPersisentCache(getInput());
    }

  //  public FilterOperator(PlanNode node) {
  //      super(node, OperatorType.FILTER);
  //  }


    public FilterOperatorTree getTree() {
        return tree;
    }

    public void setTree(FilterOperatorTree tree) {
        this.tree = tree;
    }

//   @Override
   public void run2() {
       long startTime = System.nanoTime();
      inputCache = (Cache) manager.getPersisentCache(getInput());
      Cache outputCache = (Cache)manager.getPersisentCache(getOutput());

      DistributedExecutorService des = new DefaultExecutorService(inputCache);
      FilterCallableUpdated<String,Tuple> callable = new FilterCallableUpdated<>(conf.toString(),getOutput(),conf.getObject("body").getObject("qual").toString());
//      FilterCallable callable = new FilterCallable(conf.toString(),getOutput(),conf.getObject("body").getObject("qual").toString());
      DistributedTaskBuilder builder = des.createDistributedTaskBuilder( callable);
      builder.timeout(1, TimeUnit.HOURS);
      DistributedTask task = builder.build();
      List<Future<String>> res = des.submitEverywhere(task);
      List<String> addresses = new ArrayList<String>();
      try {
         if (res != null) {
            for (Future<?> result : res) {
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

       // webCache.size() , outputCache.size() , DT

      cleanup();
      //Store Values for statistics
       updateStatistics(inputCache,null,outputCache);
   }

   @Override
    public void init(JsonObject config) {
//        super.init(conf);
        inputCache = (Cache) manager.getPersisentCache(getInput());
//        conf.putString("output",getOutput());
//        init_statistics(this.getClass().getCanonicalName());
    }

    @Override
    public void execute() {
      super.execute();
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
      mapperCallable =  new FilterCallableUpdated<>(conf.toString(),getOutput(),conf.getObject("body").getObject("qual").toString());
   }

   @Override
   public void setupReduceCallable() {

   }

   @Override
   public boolean isSingleStage() {
      return true;
   }

//    @Override
//    public String toString() {
//        return getType() + tree.toString();
//    }
}
