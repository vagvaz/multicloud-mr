package tests;

import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.web.QueryStatus;
import eu.leads.processor.web.WebServiceClient;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.net.MalformedURLException;

/**
 * Created by Apostolos Nydriotis on 2015/07/03.
 */
public class SumbitCountMinTest {
  private static String host;
  private static int port;
  private static final String CACHE_NAME = "clustered";

  public static void main(String[] args) {
    host = "http://localhost";
    port = 8080;
    double epsilon = 0.9;
    double delta = 0.02;

    if (args.length > 1) {
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

    int[] wd = calculateSketchDimentions(delta, epsilon);

    LQPConfiguration.initialize();

    JsonObject jsonObject = new JsonObject();
    jsonObject.putObject("operator", new JsonObject());
    jsonObject.getObject("operator").putObject("configuration", new JsonObject());
    jsonObject.getObject("operator").putString("name", "countMin");
    jsonObject.getObject("operator").putArray("inputs", new JsonArray().add(CACHE_NAME));
    jsonObject.getObject("operator").putString("output", "testOutputCache");
    jsonObject.getObject("operator").putArray("inputMicroClouds",
                                              new JsonArray().add("localcluster"));
    jsonObject.getObject("operator").putArray("outputMicroClouds",
                                              new JsonArray().add("localcluster"));
    jsonObject.getObject("operator")
        .putObject("scheduling", new JsonObject().putString("localcluster",
                                                            LQPConfiguration
                                                                .getInstance()
                                                                .getConfiguration()
                                                                .getString("node.ip")))
        .putString("reduceLocal", "true")
        .getObject("configuration").putNumber("w", wd[0]).putNumber("d", wd[1])
    ;
    jsonObject.getObject("operator").putObject("targetEndpoints",
                                               new JsonObject().putString("localcluster",
                                                                          LQPConfiguration
                                                                              .getInstance()
                                                                              .getConfiguration()
                                                                              .getString(
                                                                                  "node.ip")));
    try {
      String[] lines = {"this is a line",
                        "arnaki aspro kai paxy",
                        "ths manas to kamari",
                        "this another line is yoda said",
                        "rudolf to elafaki",
                        "na fame pilafaki"};

      JsonObject data = new JsonObject();
      for (int i = 0; i < 1000; i++) {
        data.putString(String.valueOf(i), lines[i % lines.length]);
      }
      WebServiceClient.putObject("clustered", "id", data);  // Add data to the input cache

      QueryStatus res = WebServiceClient.executeMapReduceJob(jsonObject, host + ":" + port);
      String id = res.getId();
      System.out.println("Submitted job. id: " + id);
      System.out.print("Executing");

      while (true) {
        QueryStatus status = WebServiceClient.getQueryStatus(id);
        if (status.getStatus().equals("COMPLETED")) {
          break;
        } else if (status.getErrorMessage() != null && status.getErrorMessage().length() > 0) {
          System.out.println(status.getErrorMessage());
          break;
        } else {
          System.out.print(".");
          Thread.sleep(500);
        }
      }

      printResults(id);

      System.out.println("\nDONE");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static RemoteCacheManager createRemoteCacheManager(String host) {
    ConfigurationBuilder builder = new ConfigurationBuilder();
    builder.addServer().host(host).port(11222);
    return new RemoteCacheManager(builder.build());
  }

  private static void printResults(String id) {
    System.out.println("\n\nlocalcluster");
    RemoteCacheManager remoteCacheManager = createRemoteCacheManager(LQPConfiguration
                                                                         .getInstance()
                                                                         .getConfiguration()
                                                                         .getString(
                                                                             "node.ip"));
    RemoteCache results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);

//    System.out.println("dresden");
//    remoteCacheManager = createRemoteCacheManager(DRESDEN2_IP);
//    results = remoteCacheManager.getCache(id);
//    PrintUtilities.printMap(results);

//    System.out.println("hamm5");
//    remoteCacheManager = createRemoteCacheManager(HAMM5_IP);
//    results = remoteCacheManager.getCache(id);
//    PrintUtilities.printMap(results);
//
//    System.out.println("hamm6");
//    remoteCacheManager = createRemoteCacheManager(HAMM6_IP);
//    results = remoteCacheManager.getCache(id);
//    PrintUtilities.printMap(results);
  }


  private static int[] calculateSketchDimentions(double delta, double epsilon) {
    int[] wd = new int[2];
    wd[0] = (int) Math.ceil(Math.E / epsilon);
    wd[1] = (int) Math.ceil(Math.log(1d / delta));
    return wd;
  }
}
