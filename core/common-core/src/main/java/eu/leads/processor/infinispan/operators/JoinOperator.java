package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsMapperCallable;
import eu.leads.processor.math.FilterOperatorTree;
import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskBuilder;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/7/13
 * Time: 8:34 AM
 * To change this template use File | Settings | File Templates.
 */

public class JoinOperator extends MapReduceOperator {

  private FilterOperatorTree tree;
  private String innerCacheName;
  private String outerCacheName;
  private Cache inputCache2;
  private Map<String, List<String>> tableCols;
  private LogProxy logProxy;
  private String qualString;
  private String leftTableName;
  private String rightTableName;

  private boolean isLeft;

  public JoinOperator(Node com, InfinispanManager persistence, LogProxy log, Action action) {
    super(com, persistence, log, action);

    JsonElement qual = conf.getObject("body").getElement("joinQual");
    if (!qual.asObject().getString("type").equals("EQUAL")) {
      //TODO change to logProxy
      log.error("JOIN is not equal but " + qual.asObject().getString("type"));
    }
    tree = new FilterOperatorTree(qual);
    tableCols = tree.getJoinColumns();
    findTableNames();
  }

  private void findTableNames() {
    //find left table name innerCache
    JsonObject leftSchema = conf.getObject("body").getObject("leftSchema");
    JsonObject fields = leftSchema.getObject("fieldsByQualifiedName");
    for (String f : fields.getFieldNames()) {
      if (f.contains(".")) {
        String tableName = f.substring(0, f.lastIndexOf("."));
        if (tableCols.containsKey(tableName)) {
          leftTableName = tableName;
          break;
        }
      }
    }

    JsonObject rightSchema = conf.getObject("body").getObject("rightSchema");
    JsonObject rfields = rightSchema.getObject("fieldsByQualifiedName");
    for (String f : rfields.getFieldNames()) {
      if (f.contains(".")) {
        String tableName = f.substring(0, f.lastIndexOf("."));
        if (tableCols.containsKey(tableName)) {
          rightTableName = tableName;
          break;
        }
      }
    }

  }


  @Override public void init(JsonObject config) {
    //        super.init(config); //fix set correctly caches names
    //fix configuration
    //        JsonObject correctQual = resolveQual(conf);
    //        conf.getObject("body").putObject("joinQual",correctQual);
    JsonArray inputsArray = action.getData().getObject("operator").getArray("inputs");
    Iterator<Object> inputIterator = inputsArray.iterator();
    List<String> inputs = new ArrayList<String>(2);
    while (inputIterator.hasNext()) {
      inputs.add((String) inputIterator.next());
    }
    Cache left = (Cache) manager.getPersisentCache(inputs.get(0));
    Cache right = (Cache) manager.getPersisentCache(inputs.get(1));
    //       if(left.size() >= right.size()){
    innerCacheName = left.getName();
    outerCacheName = right.getName();
    isLeft = true;
    //       }
    //       else{
    //           innerCacheName = right.getName();
    //           outerCacheName = left.getName();
    //           isLeft = false;
    //       }
    conf.putString("output", getOutput());
    init_statistics(this.getClass().getCanonicalName());
    super.init(config);
  }

  private JsonObject resolveQual(JsonObject conf) {
    JsonObject qual = conf.getObject("body").getObject("joinQual");
    JsonObject leftSchema = conf.getObject("body").getObject("leftSchema");
    JsonObject rightSchema = conf.getObject("body").getObject("rightSchema");
    JsonObject leftExpr = qual.getObject("body").getObject("leftExpr");
    JsonObject rightExpr = qual.getObject("body").getObject("rightExpr");
    boolean swap = true;
    String leftFieldName = leftExpr.getObject("body").getObject("column").getString("name");
    //if(leftSchema == null || leftSchema.getArray("fields") == null)
    if (leftSchema == null || !leftSchema.containsField("fields") || leftSchema.getArray("fields") == null)
      return qual;
    Iterator<Object> iterator = leftSchema.getArray("fields").iterator();
    if (leftSchema == null || !leftSchema.containsField("fields") || leftSchema.getArray("fields") == null)
      return qual;
    while (iterator.hasNext()) {
      JsonObject field = (JsonObject) iterator.next();
      if (field.getString("name").equals(leftFieldName)) {
        swap = false;
        break;
      }
    }
    if (swap) {
      log.info("Join  swap predicates");
      conf.getObject("body").getObject("joinQual").getObject("body").putObject("leftExpr", rightExpr);
      conf.getObject("body").getObject("joinQual").getObject("body").putObject("rightExpr", leftExpr);
    }
    log.info("Join Did not need to swap predicates");
    return conf.getObject("body").getObject("joinQual");
  }

  @Override public void execute() {
    super.execute();

  }

  @Override public void cleanup() {
    super.cleanup();
  }

  @Override public String getContinuousListenerClass() {
    return null;
  }

  //   @Override
  //   public void createCaches(boolean isRemote, boolean executeOnlyMap, boolean executeOnlyReduce) {
  //      Set<String> targetMC = getTargetMC();
  //      for(String mc : targetMC){
  //         createCache(mc,getOutput());
  //      }
  //   }

  @Override public void setupMapCallable() {
    inputCacheName = innerCacheName;
    inputCache = (Cache) manager.getPersisentCache(innerCacheName);
    inputCache2 = (Cache) manager.getPersisentCache(outerCacheName);
    JsonObject ob = new JsonObject();
    for (Map.Entry<String, List<String>> entry : tableCols.entrySet()) {
      JsonArray array = new JsonArray();
      for (String col : entry.getValue()) {
        array.add(col);
      }
      ob.putArray(entry.getKey(), array);
    }
    conf.putObject("joinColumns", ob);
    conf.putString("inputCache", leftTableName);
    setMapper(new JoinMapper(conf.toString()));
    super.setupMapCallable();
    //      mapperCallable = new JoinCallableUpdated(conf.toString(),getOutput(),
    //                                                      ,
    //                                                      isLeft);

  }

  @Override public void localExecuteMap() {

    List<Future<String>> res = new ArrayList<>();
    List<String> addresses = new ArrayList<String>();

    if (mapperCallable != null) {
      if (inputCache.size() == 0) {
        replyForSuccessfulExecution(action);
        return;
      }
      DistributedExecutorService des = new DefaultExecutorService(inputCache);
      //      ScanCallable callable = new ScanCallable(conf.toString(),getOutput());

      setMapperCallableEnsembleHost();
      DistributedTaskBuilder builder = des.createDistributedTaskBuilder(mapperCallable);
      builder.timeout(1, TimeUnit.HOURS);
      DistributedTask task = builder.build();
      res = des.submitEverywhere(task);
      //      Future<String> res = des.submit(callable);

    }
    //    replyForSuccessfulExecution(action);

    System.out.println("####### RUNNING 2nd CALLABLE #########");
    collector = new LeadsCollector(0, intermediateCacheName);
    conf.putString("inputCache", rightTableName);
    mapperCallable = new LeadsMapperCallable((Cache) inputCache2, collector, new JoinMapper(conf.toString()),
        LQPConfiguration.getInstance().getMicroClusterName());



    if (mapperCallable != null) {
      //      if (inputCache2.size() == 0) {
      //        replyForSuccessfulExecution(action);
      //        return;
      //      }
      DistributedExecutorService des = new DefaultExecutorService(inputCache2);

      //      ScanCallable callable = new ScanCallable(conf.toString(),getOutput());

      setMapperCallableEnsembleHost();
      DistributedTaskBuilder builder = des.createDistributedTaskBuilder(mapperCallable);
      builder.timeout(1, TimeUnit.HOURS);
      DistributedTask task = builder.build();
      List<Future<String>> res2 = des.submitEverywhere(task);
      //      Future<String> res = des.submit(callable);
      /**
       * READ RESULT FROM MAP 1
       */
      try {
        if (res != null) {
          double perc = handleCallables(res, mapperCallable);
          System.out.println("map " + mapperCallable.getClass().toString() +
              " Execution is done " + perc);
          log.info("map " + mapperCallable.getClass().toString() +
              " Execution is done " + perc);
        } else {
          System.out.println("map " + mapperCallable.getClass().toString() +
              " Execution not done ");
          log.info("map " + mapperCallable.getClass().toString() +
              " Execution not done");
          failed = true;
          //          replyForFailExecution(action);
        }
      } catch (InterruptedException e) {
        log.error("Exception in Map Excuettion " + "map " + mapperCallable.getClass().toString() + "\n" +
            e.getClass().toString());
        log.error(e.getMessage());
        System.err.println("Exception in Map Excuettion " + "map " + mapperCallable.getClass().toString() + "\n" +
                e.getClass().toString());
        System.err.println(e.getMessage());
        log.error(e.getStackTrace().toString());
        e.printStackTrace();
        failed = true;
        //        replyForFailExecution(action);
      } catch (ExecutionException e) {
        log.error("Exception in Map Excuettion " + "map " + mapperCallable.getClass().toString() + "\n" +
            e.getClass().toString());
        log.error(e.getMessage());
        System.err.println("Exception in Map Excuettion " + "map " + mapperCallable.getClass().toString() + "\n" +
                e.getClass().toString());
        System.err.println(e.getMessage());
        log.error(e.getStackTrace().toString());
        e.printStackTrace();
        failed = true;
        //        replyForFailExecution(action);
      }



      try {
        if (res2 != null) {
          double perc = handleCallables(res2, mapperCallable);
          System.out.println("map " + mapperCallable.getClass().toString() +
              " Execution is done " + perc);
          log.info("map " + mapperCallable.getClass().toString() +
              " Execution is done " + perc);
        } else {
          System.out.println("map " + mapperCallable.getClass().toString() +
              " Execution not done");
          log.info("map " + mapperCallable.getClass().toString() +
              " Execution not done");
          failed = true;
          replyForFailExecution(action);
        }
      } catch (InterruptedException e) {
        log.error("Exception in Map Excuettion " + "map " + mapperCallable.getClass().toString() + "\n" +
            e.getClass().toString());
        log.error(e.getMessage());
        System.err.println("Exception in Map Excuettion " + "map " + mapperCallable.getClass().toString() + "\n" +
                e.getClass().toString());
        System.err.println(e.getMessage());
        e.printStackTrace();
        log.error(e.getStackTrace().toString());
        failed = true;
        replyForFailExecution(action);
      } catch (ExecutionException e) {
        log.error("Exception in Map Excuettion " + "map " + mapperCallable.getClass().toString() + "\n" +
            e.getClass().toString());
        log.error(e.getMessage());
        System.err.println("Exception in Map Excuettion " + "map " + mapperCallable.getClass().toString() + "\n" +
                e.getClass().toString());
        System.err.println(e.getMessage());
        e.printStackTrace();
        log.error(e.getStackTrace().toString());
        failed = true;
        replyForFailExecution(action);
      }
    }
    replyForSuccessfulExecution(action);
  }

  @Override public void setupReduceCallable() {
    setReducer(new JoinReducer(conf.toString()));
    super.setupReduceCallable();
  }

}
