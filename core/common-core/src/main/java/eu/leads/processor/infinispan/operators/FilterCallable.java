package eu.leads.processor.infinispan.operators;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.math.FilterOperatorTree;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;
import java.util.*;

/**
 * Created by vagvaz on 9/24/14.
 */
public class FilterCallable<K, V> implements DistributedCallable<K, V, String>, Serializable {
  transient protected Cache<K, V> inputCache;
  transient protected Cache outputCache;
  transient protected FilterOperatorTree tree;
  transient protected JsonObject inputSchema;
  transient protected JsonObject outputSchema;
  transient protected Map<String, String> outputMap;
  transient protected Map<String, List<JsonObject>> targetsMap;
  transient protected JsonObject conf;
  protected String configString;
  protected String output;
  protected String qualString;

  public FilterCallable(String configString, String output, String qualString) {
    this.configString = configString;
    this.output = output;
    this.qualString = qualString;
  }

  @Override public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
    inputCache = cache;
    outputCache = cache.getCacheManager().getCache(output);
    JsonObject object = new JsonObject(qualString);
    conf = new JsonObject(configString);
    tree = new FilterOperatorTree(object);
    outputSchema = conf.getObject("body").getObject("outputSchema");
    inputSchema = conf.getObject("body").getObject("inputSchema");
    targetsMap = new HashMap();
    outputMap = new HashMap<>();
    JsonArray targets = conf.getObject("body").getArray("targets");
    if (conf.containsField("body") && conf.getObject("body").containsField("targets")) {
      Iterator<Object> targetIterator = targets.iterator();
      while (targetIterator.hasNext()) {
        JsonObject target = (JsonObject) targetIterator.next();
        List<JsonObject> tars =
            targetsMap.get(target.getObject("expr").getObject("body").getObject("column").getString("name"));
        if (tars == null) {
          tars = new ArrayList<>();
        }
        tars.add(target);
        targetsMap.put(target.getObject("expr").getObject("body").getObject("column").getString("name"), tars);
      }
    }
    //      JsonArray targets = conf.getObject("body").getArray("targets");
    //      Iterator<Object> targetIterator = targets.iterator();
    //      while (targetIterator.hasNext()) {
    //         JsonObject target = (JsonObject) targetIterator.next();
    //         targetsMap.put(target.getObject("expr").getObject("body").getObject("column").getString("name"), target);
    //      }
  }

  @Override public String call() throws Exception {
    final ClusteringDependentLogic cdl =
        inputCache.getAdvancedCache().getComponentRegistry().getComponent(ClusteringDependentLogic.class);
    for (Object ikey : inputCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet()) {
      if (!cdl.localNodeIsPrimaryOwner(ikey))
        continue;
      String key = (String) ikey;
      //          String value = (String)inputCache.get(key);
      Tuple tuple = (Tuple) inputCache.get(key);

      //         Tuple tuple = new Tuple(value);
      if (tree.accept(tuple)) {
        //            tuple = prepareOutput(tuple);
        outputCache.put(key, tuple);
      }

    }
    return inputCache.getCacheManager().getAddress().toString();
  }

  protected Tuple prepareOutput(Tuple tuple) {
    if (outputSchema.toString().equals(inputSchema.toString())) {
      return tuple;
    }

    JsonObject result = new JsonObject();
    //WARNING
    //       System.err.println("out: " + tuple.asString());

    if (targetsMap.size() == 0) {
      //          System.err.println("s 0 ");
      return tuple;

    }
    //       System.err.println("normal");

    //END OF WANRING
    List<String> toRemoveFields = new ArrayList<String>();
    Map<String, List<String>> toRename = new HashMap<String, List<String>>();
    for (String field : tuple.getFieldNames()) {
      List<JsonObject> ob = targetsMap.get(field);
      if (ob == null)
        toRemoveFields.add(field);
      else {
        for (JsonObject obb : ob) {
          List<String> ren = toRename.get(field);
          if (ren == null) {
            ren = new ArrayList<>();
          }
          //               toRename.put(field, ob.getObject("column").getString("name"));
          ren.add(obb.getObject("column").getString("name"));
          toRename.put(field, ren);
        }
      }
    }
    tuple.removeAtrributes(toRemoveFields);
    tuple.renameAttributes(toRename);
    return tuple;
  }

  protected void handlePagerank(Tuple t) {

    if (t.hasField("default.webpages.pagerank")) {
      if (!t.hasField("url"))
        return;
      String pagerankStr = t.getAttribute("pagerank");
      //            Double d = Double.parseDouble(pagerankStr);
      //            if (d < 0.0) {
      //
      //                try {
      ////                    d = LeadsPrGraph.getPageDistr(t.getAttribute("url"));
      //                    d = (double) LeadsPrGraph.getPageVisitCount(t.getAttribute("url"));
      //
      //                } catch (IOException e) {
      //                    e.printStackTrace();
      //                }
      //                t.setAttribute("pagerank", d.toString());
      //        }
    }
  }
}
