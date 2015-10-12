package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.common.infinispan.AcceptAllFilter;
import eu.leads.processor.common.infinispan.EnsembleCacheUtils;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsMapper;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 9/26/14.
 */
public class WGSMapper extends LeadsMapper<String, String, String, String> {
  public WGSMapper(String configString) {
    super(configString);
  }

  protected transient String prefix;
  protected transient List<String> attributes;
  protected transient BasicCache webCache;
  protected transient BasicCache outputCache;
  protected transient int depth;
  protected transient int iteration;
  protected transient double totalSum;
  protected transient BasicCache pagerankCache;
  protected transient InfinispanManager imanager;
  protected transient Logger log;
  protected transient EnsembleCacheManager readManager;

  public WGSMapper() {
  }

  @Override public void initialize() {
    imanager = InfinispanClusterSingleton.getInstance().getManager();
    isInitialized = true;
    super.initialize();
    totalSum = -1.0;
    iteration = conf.getInteger("iteration");
    depth = conf.getInteger("depth");
    readManager = new EnsembleCacheManager(conf.getString("outputEnsembleHost"));
    readManager.start();
    //      webCache = (Cache) imanager.getPersisentCache(conf.getString("webCache"));
    webCache = readManager.getCache(conf.getString("webCache"), new ArrayList(readManager.sites()),
        EnsembleCacheManager.Consistency.DIST);
    pagerankCache = readManager
        .getCache("pagerankCache", new ArrayList(readManager.sites()), EnsembleCacheManager.Consistency.DIST);
    if (iteration < depth) {
      outputCache = readManager.getCache(conf.getString("outputCache"), new ArrayList(readManager.sites()),
          EnsembleCacheManager.Consistency.DIST);
    } else {
      outputCache = null;
    }

    prefix = webCache.getName() + ":";
    JsonArray array = conf.getArray("attributes");
    Iterator<Object> iterator = array.iterator();
    attributes = new ArrayList<String>(array.size());
    while (iterator.hasNext()) {
      attributes.add((String) iterator.next());
    }
    LQPConfiguration.initialize();
    log = LoggerFactory.getLogger(WGSMapper.class);
  }

  @Override public void map(String key, String value, Collector<String, String> collector) {
    if (!isInitialized)
      this.initialize();
    //      String jsonString = (String) webCache.get(prefix+key);
    //      if (jsonString==null || jsonString.equals("")){
    //         return;
    //      }
    System.err.println("Running map for " + key.toString());
    Object oo = webCache.get(prefix + key);
    Tuple webpage = null;
    if (oo instanceof Tuple) {
      webpage = (Tuple) oo;
    } else {
      //      System.err.println("\n\n\n\nSERIOUS ERROR WITH SERIALIZE GOT byte buffer " + oo.getClass().toString()  );
      //      Byte[] bytes = (Byte[]) oo;
      try {
        //        System.err.println("\n\n\n\nSERIOUS ERROR WITH SERIALIZE GOT byte buffer " + bytes.length);
      } catch (Exception e) {
        System.err.println("\n\n\n\nSERIOUS ERROR WITH SERIALIZE GOT byte buffer ");
        e.printStackTrace();
      }
      return;
      //      byte[] bytes = (byte[]) oo;
      //      ByteArrayInputStream bs = new ByteArrayInputStream(bytes);
      //      try {
      //        ObjectInputStream ois = new ObjectInputStream(bs);
      //        BasicBSONDecoder decoder = new BasicBSONDecoder();
      //        BSONObject bsonObject =  decoder.readObject(ois);
      //        webpage = new Tuple(bsonObject.toString());
      //        System.err.println("\n\n\n\nSERIOUS ERROR WITH SERIALIZE GOT byte buffer unserialized " + webpage.toString());
      //
      //      } catch (IOException e) {
      //        e.printStackTrace();
      //      }
    }
    //      Tuple t = new Tuple(jsonString);
    if (webpage == null) {
      System.err.println("WAS NULLL " + key.toString());
      return;
    }
    Tuple t = new Tuple(webpage);
    handlePagerank(t);
    JsonObject result = new JsonObject();
    result.putString("url", t.getAttribute("url"));
    result.putString("pagerank", computePagerank(result.getString("url")));
    result.putString("sentiment", t.getGenericAttribute("sentiment").toString());
    //    result.putString("microCluster",LQPConfiguration.getInstance().getMicroClusterName());
    ArrayList<String> microclouds = new ArrayList<>();
    microclouds.add("hamm5");
    microclouds.add("hamm6");
    microclouds.add("dresden2");

    int mcIndex = Math.abs((t.getAttribute("url").hashCode())) % microclouds.size();
    result.putString("micro-cluster", microclouds.get(mcIndex));
    ArrayList<Object> linksArray = (ArrayList<Object>) t.getGenericAttribute("links");
    JsonArray array = new JsonArray();
    for (Object o : linksArray) {
      log.error("ADDING TO LINKS " + o.toString());
      array.add(o.toString());
    }
    result.putValue("links", array);
    System.err.println("WGS TUPLE " + result.toString());
    collector.emit(String.valueOf(iteration), result.toString());
    if (outputCache != null) {
      if (!result.getElement("links").isArray()) {
        log.error("SERIOUS ERROR links is not an array WGSMAPPER");
        return;
      }
      JsonArray links = result.getArray("links");
      Iterator<Object> iterator = links.iterator();
      while (iterator.hasNext()) {
        String link = (String) iterator.next();
        log.error("Inserting into next iteration cache " + outputCache.getName() + " l " + link);
        System.err.println("OUTOUTING WGS " + outputCache.getName() + " l  " + link);
        EnsembleCacheUtils.putToCache(outputCache, link, link);
      }
    }

  }

  private String computePagerank(String url) {
    double result = 0.0;
    if (totalSum < 0) {
      computeTotalSum();
    }
    try {


      return Double.toString(result);
    } catch (Exception e) {
      return Double.toString((10000 / url.length()) / 10000);

    }
  }

  private void computeTotalSum() {
    Cache approxSumCache = (Cache) imanager.getPersisentCache("approx_sum_cache");
    CloseableIterable<Map.Entry<String, Integer>> iterable =
        approxSumCache.getAdvancedCache().filterEntries(new AcceptAllFilter());

    for (Map.Entry<String, Integer> outerEntry : iterable) {
      totalSum += outerEntry.getValue();
    }
    if (totalSum > 0) {
      totalSum += 1;
    }
  }
}
