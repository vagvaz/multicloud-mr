package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.math.FilterOperatorTree;
import org.infinispan.Cache;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 11/21/14.
 */
public class JoinOperator2 extends MapReduceOperator {
   private FilterOperatorTree tree;
   private String innerCacheName;
   private String outerCacheName;
   private LogProxy logProxy;
   private String qualString;
   private Cache tmpInputCache;
   private boolean isLeft;
   public JoinOperator2(Node com, InfinispanManager persistence, LogProxy log, Action action) {
      super(com, persistence, log, action);
   }

   @Override
   public void init(JsonObject config) {
      {
        super.init(config); //fix set correctly caches names
         //fix configuration
//        JsonObject correctQual = resolveQual(conf);
//        conf.getObject("body").putObject("joinQual",correctQual);
         JsonArray inputsArray = action.getData().getObject("operator").getArray("inputs");
         Iterator<Object> inputIterator = inputsArray.iterator();
         List<String> inputs = new ArrayList<String>(2);
         while(inputIterator.hasNext()){
            inputs.add((String)inputIterator.next());
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
         conf.putString("output",getOutput());
         Cache outputCache = (Cache) manager.getPersisentCache(getOutput());
        tmpInputCache  = (Cache)manager.getPersisentCache(action.getId());
         tmpInputCache.put(innerCacheName,innerCacheName);
         tmpInputCache.put(outerCacheName,outerCacheName);
         inputCache = tmpInputCache;
         Map<String,List<String>> columnsByTable = computeColumnsByTable();
//         mapper = new JoinMapper(conf.toString(),getOutput());
//         reducer = new JoinReducer(conf.toString(),getOutput());
         init_statistics(this.getClass().getCanonicalName());
      }
   }

   private Map<String, List<String>> computeColumnsByTable() {
      Map<String,List<String>> result = null;

      return result;
   }

   @Override
   public void run() {
      super.run();
   }

   @Override
   public void execute() {
      super.execute();
   }

   @Override
   public void cleanup() {
      super.cleanup();
   }


}
