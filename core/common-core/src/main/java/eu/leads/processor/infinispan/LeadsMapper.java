package eu.leads.processor.infinispan;

import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.core.Tuple;
import org.apache.commons.configuration.XMLConfiguration;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.manager.EmbeddedCacheManager;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.Serializable;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 5:58 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class LeadsMapper<kIN, vIN, kOut, vOut> implements Mapper<kIN, vIN, kOut, vOut>,Serializable {
  /**
   *
   */
  private static final long serialVersionUID = -1040739216725664106L;

  protected String configString;
  //        protected Cache<String,String> cache;
  transient protected JsonObject conf;
  transient protected boolean isInitialized = false;
  //    transient protected long overall;
  //    transient  protected Timer timer;
  //   transient  protected ProgressReport report;
  transient  protected JsonObject inputSchema;
  transient  protected JsonObject outputSchema;
  transient  protected Map<String,String> outputMap;
  transient  protected Map<String,List<JsonObject>> targetsMap;
  transient protected EmbeddedCacheManager manager;
  transient protected XMLConfiguration xmlConfiguration;

  public LeadsMapper() {

  }

  public LeadsMapper(JsonObject configuration) {
    this.conf = configuration;
  }
  public LeadsMapper(String configString){this.configString = configString;}

  public void  setCacheManager(EmbeddedCacheManager manager){
    this.manager = manager;
  }

  public void initialize(XMLConfiguration xmlConfiguration) {
    this.xmlConfiguration = xmlConfiguration;

  }

  public void initialize() {

    conf = new JsonObject(configString);
    if(conf.containsField("body") && conf.getObject("body").containsField("outputSchema")) {
      outputSchema = conf.getObject("body").getObject("outputSchema");
      inputSchema = conf.getObject("body").getObject("inputSchema");
      targetsMap = new HashMap();
      outputMap = new HashMap<>();
      JsonArray targets = conf.getObject("body").getArray("targets");
      Iterator<Object> targetIterator = targets.iterator();
      while (targetIterator.hasNext()) {
        JsonObject target = (JsonObject) targetIterator.next();
        List<JsonObject> tars = targetsMap.get(target.getObject("expr").getObject("body").getObject("column").getString("name"));
        if (tars == null) {
          tars = new ArrayList<>();
        }
        tars.add(target);
        targetsMap.put(target.getObject("expr").getObject("body").getObject("column").getString("name"),tars);
      }
    }

    manager = InfinispanClusterSingleton.getInstance().getManager().getCacheManager();
    //       JsonArray fields = outputSchema.getArray("fields");
    //       Iterator<Object> fieldIterator = fields.iterator();
    //       while(fieldIterator.hasNext()){
    //          JsonObject field = (JsonObject)fieldIterator.next();
    //          String outputFieldName = field.getString("name");
    //          outputMap.put(outputFieldName,targetsMap.get(outputFieldName).getObject("expr").getObject("body").getString("name"));
    //       }
    //       overall =conf.getLong("workload",100);
    //       timer = new Timer();
    //       report = new ProgressReport(this.getClass().toString(), 0, overall);
    //       timer.scheduleAtFixedRate(report, 0, 2000);
  }


  protected void finalizeTask() {
    //        report.printReport(report.getReport());
    //////////////StdOutputWriter.getInstance().println("");
    //        report.cancel();
    //        timer.cancel();
  }

  protected void progress() {
    //        report.tick();
  }

  protected void progress(long n) {
    //        report.tick(n);
  }

  //    protected double getProgress() {
  //        return report.getReport();
  //    }

//  protected Tuple prepareOutput(Tuple tuple) {
//    if (outputSchema.toString().equals(inputSchema.toString())) {
//      return tuple;
//    }
//
//    JsonObject result = new JsonObject();
//    //WARNING
//    //       System.err.println("out: " + tuple.asString());
//
//    if(targetsMap.size() == 0)
//    {
//      //          System.err.println("s 0 ");
//      return tuple;
//
//    }
//    //       System.err.println("normal");
//
//    //END OF WANRING
//    List<String> toRemoveFields = new ArrayList<String>();
//    Map<String,List<String>> toRename = new HashMap<String,List<String>>();
//    for (String field : tuple.getFieldNames()) {
//      List<JsonObject> ob = targetsMap.get(field);
//      if (ob == null)
//        toRemoveFields.add(field);
//      else {
//        for(JsonObject obb : ob)
//        {
//          List<String> ren  = toRename.get(field);
//          if(ren == null){
//            ren = new ArrayList<>();
//          }
//          //               toRename.put(field, ob.getObject("column").getString("name"));
//          ren.add(obb.getObject("column").getString("name"));
//          toRename.put(field,ren);
//        }
//      }
//    }
//    tuple.removeAtrributes(toRemoveFields);
//    tuple.renameAttributes(toRename);
//    return tuple;
//  }
  protected  void handlePagerank(Tuple t) {

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
