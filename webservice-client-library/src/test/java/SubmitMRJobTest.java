import eu.leads.processor.common.StringConstants;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.web.QueryStatus;
import eu.leads.processor.web.WebServiceClient;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.net.MalformedURLException;

/**
 * Created by Apostolos Nydriotis on 2015/06/22.
 */
public class SubmitMRJobTest {

  private static String host;
  private static int port;

  public static void main(String[] args) {
    host = "http://localhost";
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
    jsonObject.getObject("operator").putArray("inputs", new JsonArray().add("clustered"));
    jsonObject.getObject("operator").putString("output", "testOutputCache");
//    jsonObject.getObject("operator").putArray("inputMicroClouds",
//                                              new JsonArray().add("localcluster"));
//    jsonObject.getObject("operator").putArray("outputMicroClouds",
//                                              new JsonArray().add("localcluster"));
    jsonObject.getObject("operator")
        .putObject("scheduling", new JsonObject().putString("localcluster",
                                                            LQPConfiguration
                                                                .getInstance()
                                                                .getConfiguration()
                                                                .getString("node.ip")));
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
      System.out.println("id: " + id);

      while (!WebServiceClient.getQueryStatus(id).getStatus().equals("COMPLETED")) {
        System.out.println("Sleeping");
        Thread.sleep(100);
      }

      System.out.println("status: " + res.getStatus());
      System.out.println("msg: " + res.getErrorMessage());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
