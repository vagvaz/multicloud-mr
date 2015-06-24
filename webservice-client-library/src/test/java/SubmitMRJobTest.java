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

    JsonObject data = new JsonObject();
    data.putString("1", "this is a line");
    data.putString("2", "arnaki aspro kai paxy");
    data.putString("3", "ths manas to kamari");
    data.putString("4", "this another line is yoda said");
    data.putString("5", "rudolf to elafaki");
    data.putString("6", "na fame pilafaki");

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
      WebServiceClient.putObject("clustered", "id", data);

      QueryStatus res = WebServiceClient.executeMapReduceJob(jsonObject, host + ":" + port);
      System.out.println("id: " + res.getId());
      System.out.println("status: " + res.getStatus());
      System.out.println("msg: " + res.getErrorMessage());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
