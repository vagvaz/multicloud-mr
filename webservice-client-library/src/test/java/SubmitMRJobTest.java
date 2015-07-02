import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.web.QueryStatus;
import eu.leads.processor.web.WebServiceClient;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.net.MalformedURLException;
import java.util.ArrayList;

/**
 * Created by Apostolos Nydriotis on 2015/06/22.
 */
public class SubmitMRJobTest {

  private static String host;
  private static int port;
  private static final String DRESDEN2_IP = "80.156.73.116";
  private static final String DD1A_IP = "80.156.222.4";
  private static final String HAMM5_IP = "5.147.254.161";
  private static final String HAMM6_IP = "5.147.254.199";
  private static final String CACHE_NAME = "clustered";

  public static void main(String[] args) {
//    host = "http://localhost";
//    host = "http://" + DRESDEN2_IP;  // dresden2
    host = "http://" + DD1A_IP;  // dd1a
    port = 8080;

    if (args.length == 2) {
      host = args[0];
      port = Integer.parseInt(args[1]);
    }

    try {
      if (WebServiceClient.initialize(host, port)) {
        System.out.println("Client is up");
      }
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }

    LQPConfiguration.initialize();

    JsonObject jsonObject = new JsonObject();
    jsonObject.putObject("operator", new JsonObject());
    jsonObject.getObject("operator").putObject("configuration", new JsonObject());
    jsonObject.getObject("operator").putString("name", "wordCount");
    jsonObject.getObject("operator").putArray("inputs", new JsonArray().add(CACHE_NAME));
    jsonObject.getObject("operator").putString("output", "testOutputCache");
    jsonObject.getObject("operator").putArray("inputMicroClouds",
                                              new JsonArray().add("localcluster"));
    jsonObject.getObject("operator").putArray("outputMicroClouds",
                                              new JsonArray().add("localcluster"));
//    jsonObject.getObject("operator")
//        .putObject("scheduling", new JsonObject().putString("localcluster",
//                                                            LQPConfiguration
//                                                                .getInstance()
//                                                                .getConfiguration()
//                                                                .getString("node.ip")))
//        .putString("reduceLocal", "true")
//    ;
//    jsonObject.getObject("operator").putObject("targetEndpoints",
//                                               new JsonObject().putString("localcluster",
//                                                                          LQPConfiguration
//                                                                              .getInstance()
//                                                                              .getConfiguration()
//                                                                              .getString(
//                                                                                  "node.ip")));
    jsonObject.getObject("operator")
        .putObject("scheduling",
                   new JsonObject()
                       .putArray("dresden2", new JsonArray().add(DRESDEN2_IP))
                       .putArray("dd1a", new JsonArray().add(DD1A_IP))
                       .putArray("hamm5", new JsonArray().add(HAMM5_IP))
                       .putArray("hamm6", new JsonArray().add(HAMM6_IP))
        )
        .putString("reduceLocal", "true")
        .putObject("targetEndpoints",
                   new JsonObject()
                       .putArray("dresden2", new JsonArray().add(DRESDEN2_IP))
                       .putArray("dd1a", new JsonArray().add(DD1A_IP))
                       .putArray("hamm5", new JsonArray().add(HAMM5_IP))
                       .putArray("hamm6", new JsonArray().add(HAMM6_IP))
        );
    try {

      String ensembleString = DD1A_IP + ":11222" + "|"
                              + DRESDEN2_IP + ":11222" + "|"
                              + HAMM5_IP + ":11222" + "|"
                              + HAMM6_IP + ":11222";

      EnsembleCacheManager ensembleCacheManager = new EnsembleCacheManager((ensembleString));

      EnsembleCache ensembleCache =
          ensembleCacheManager.getCache(CACHE_NAME,
                                        new ArrayList<>(ensembleCacheManager.sites()),
                                        EnsembleCacheManager.Consistency.DIST);

      String[] lines = {"this is a line",
                        "arnaki aspro kai paxy",
                        "ths manas to kamari",
                        "this another line is yoda said",
                        "rudolf to elafaki",
                        "na fame pilafaki"};

      JsonObject data;
      System.out.print("Loading data to '" + CACHE_NAME + "' cache\n ");
      data = new JsonObject();
      for (int i = 0; i < 20000; i++) {
        data.putString(String.valueOf(i), lines[i % lines.length]);
        if ((i + 1) % 100 == 0) {
          ensembleCache.put(String.valueOf(i), new Tuple(data.toString()));
          data = new JsonObject();
        }
        printProgress(i + 1);
      }

//      WebServiceClient.putObject("clustered", "id", data);  // Add data to the input cache

      System.out.println("\njsonObject = " + jsonObject.toString());

      QueryStatus res = WebServiceClient.executeMapReduceJob(jsonObject, host + ":" + port);
      String id = res.getId();
      System.out.println("Submitted job. id: " + id);
      System.out.println("Executing...");

      int secs = 0;

      while (true) {
        QueryStatus status = WebServiceClient.getQueryStatus(id);
        if (status.getStatus().equals("COMPLETED")) {
          break;
        } else if (status.getErrorMessage() != null && status.getErrorMessage().length() > 0) {
          System.out.println(status.getErrorMessage());
          break;
        } else {
          System.out.print("\r" + secs++);
          Thread.sleep(1000);
        }
      }

      printResults(id);
      printResults("metrics");

      System.out.println("\nDONE IN: " + secs + " sec");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static RemoteCacheManager createRemoteCacheManager(String host) {
    ConfigurationBuilder builder = new ConfigurationBuilder();
    builder.addServer().host(host).port(11222);
    return new RemoteCacheManager(builder.build());
  }

  private static void printProgress(int i) {
    if (i % 6 == 0 || i % 6 == 3) {
      System.out.print("\r" + i + " |");
    } else if (i % 6 == 1) {
      System.out.print("\r" + i + " \\");
    } else if (i % 6 == 2 || i % 6 == 5) {
      System.out.print("\r" + i + " \u2014");
    } else if (i % 6 == 4) {
      System.out.print("\r" + i + " /");
    }
  }

  private static void printResults(String id) {
    System.out.println("\n\ndd1a");
    RemoteCacheManager remoteCacheManager = createRemoteCacheManager(DD1A_IP);
    RemoteCache results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);

    System.out.println("dresden");
    remoteCacheManager = createRemoteCacheManager(DRESDEN2_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);

    System.out.println("hamm5");
    remoteCacheManager = createRemoteCacheManager(HAMM5_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);

    System.out.println("hamm6");
    remoteCacheManager = createRemoteCacheManager(HAMM6_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);
  }

}
