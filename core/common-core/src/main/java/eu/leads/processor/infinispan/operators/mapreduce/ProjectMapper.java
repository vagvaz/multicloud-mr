package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.common.infinispan.ClusterInfinispanManager;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsMapper;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Collector;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 8:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class ProjectMapper extends LeadsMapper<String, Tuple, String, Tuple> implements Serializable {
    transient private Cache<String, Tuple> output = null;
    transient private String prefix = "";


   public ProjectMapper(JsonObject configuration) {
        super(configuration);
    }
   public ProjectMapper(String configString){super(configString);}
    transient protected InfinispanManager imanager;
    @Override
    public void initialize() {
        isInitialized = true;
        super.initialize();

        imanager = new ClusterInfinispanManager(manager);
        prefix = conf.getString("output") + ":";
        output = (Cache<String, Tuple>) imanager.getPersisentCache(conf.getString("output"));
    }

    @Override
    public void map(String key, Tuple value, Collector<String, Tuple> collector) {
//        if (!isInitialized)
//            initialize();
//
//        progress();
//        String tupleId = key.substring(key.indexOf(':') + 1);
//        Tuple projected = value;
////        Tuple projected = new Tuple(value);
//        handlePagerank(projected);
//        projected = prepareOutput(projected);
////        output.put(prefix + tupleId, projected.asString());
//        output.put(prefix + tupleId, projected);
    }

   protected Tuple prepareOutput(Tuple tuple) {
      if (outputSchema.toString().equals(inputSchema.toString())) {
         return tuple;
      }

      JsonObject result = new JsonObject();
      //WARNING
//       System.err.println("out: " + tuple.asString());

      if(targetsMap.size() == 0)
      {
//          System.err.println("s 0 ");
         return tuple;

      }
//       System.err.println("normal");

      //END OF WANRING
      List<String> toRemoveFields = new ArrayList<String>();
      Map<String,List<String>> toRename = new HashMap<String,List<String>>();
      for (String field : tuple.getFieldNames()) {
         List<JsonObject> ob = targetsMap.get(field);
         if (ob == null)
            toRemoveFields.add(field);
         else {
            for(JsonObject obb : ob)
            {
               List<String> ren  = toRename.get(field);
               if(ren == null){
                  ren = new ArrayList<>();
               }
//               toRename.put(field, ob.getObject("column").getString("name"));
               ren.add(obb.getObject("column").getString("name"));
               toRename.put(field,ren);
            }
         }
      }
      tuple.removeAtrributes(toRemoveFields);
      tuple.renameAttributes(toRename);
      return tuple;
   }
}
