import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.plugins.PluginPackage;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.plugins.EventType;
import eu.leads.processor.web.WebServiceClient;
import org.infinispan.Cache;

import java.io.IOException;
import java.net.MalformedURLException;

/**
 * Created by vagvaz on 8/23/14.
 */
public class WebServiceClientTestPlugins {
  private static String host;
  private static int port;

  public static void main(String[] args) throws IOException {
    host = "http://localhost";
    port = 8080;
    if (args.length == 2) {
      host = args[0];
      port = Integer.parseInt(args[1]);
    }

    try {
      if (WebServiceClient.initialize(host, port))
        System.out.println("Server is Up");

    } catch (MalformedURLException e) {
      e.printStackTrace();
    }

    PluginPackage plugin = new PluginPackage("eu.leads.processor.plugins.transform.TransformPlugin",
        "eu.leads.processor.plugins.transform.TransformPlugin",
        "/home/vagvaz/Projects/idea/transform-plugin/target/transform-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar",
        "/home/vagvaz/Projects/idea/transform-plugin/transform-plugin-conf.xml");


    //upload plugin
    WebServiceClient.submitPlugin("vagvaz", plugin);
    System.out.println("Uploaded plugin");
    //distributed deployment  ( plugin id, cache to install, events)
    //PluginManager.deployPlugin();
    //      PluginManager.deployPlugin("eu.leads.processor.plugins.transform.TranformPlugin", "webpages",
    //                                  EventType.CREATEANDMODIFY,"vagvaz");

        /*Start putting values to the cache */
    WebServiceClient
        .deployPlugin("vagvaz", "eu.leads.processor.plugins.transform.TransformPlugin", null, "default.webpages",
            EventType.CREATEANDMODIFY);
    LQPConfiguration.initialize();
    //Put some configuration properties for crawler
    LQPConfiguration.getConf().setProperty("crawler.seed",
        "http://www.bbc.co.uk"); //For some reason it is ignored news.yahoo.com is used by default
    LQPConfiguration.getConf().setProperty("crawler.depth", 3);
    //Set desired target cache
    LQPConfiguration.getConf().setProperty(StringConstants.CRAWLER_DEFAULT_CACHE, "default.webpages");
    //start crawler

    //Sleep for an amount of time to test if everything is working fine
    try {
      int sleepingPeriod = 30;
      Thread.sleep(sleepingPeriod * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    //Iterate through local cache entries to ensure things went as planned
    Cache cache = (Cache) InfinispanClusterSingleton.getInstance().getManager().getPersisentCache("mycache");
    PrintUtilities.printMap(cache);

	    /*Cleanup and close....*/
    //      PersistentCrawl.stop();
    System.out.println("Local cache " + cache.entrySet().size() + " --- global --- " + cache.size());
    InfinispanClusterSingleton.getInstance().getManager().stopManager();
    System.exit(0);
  }

}
