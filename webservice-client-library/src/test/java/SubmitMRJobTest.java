import eu.leads.processor.web.ActionResult;
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

    JsonObject jsonObject = new JsonObject();
    jsonObject.putObject("operator", new JsonObject());
    jsonObject.getObject("operator").putObject("configuration", new JsonObject());
    jsonObject.getObject("operator").getObject("configuration").putString("name", "wordCount");
    jsonObject.getObject("operator").putArray("inputs", new JsonArray().add("clustered"));
    jsonObject.getObject("operator").putString("output", "testOutputCache");
    jsonObject.getObject("operator").putArray("inputMicroClouds",
                                              new JsonArray().add("localcluster"));
    jsonObject.getObject("operator").putArray("outputMicroClouds",
                                              new JsonArray().add("localcluster"));

    try {
      QueryStatus res = WebServiceClient.executeMapReduceJob(jsonObject, host + ":" + port);
      System.out.println("id: " + res.getId());
      System.out.println("status: " + res.getStatus());
      System.out.println("msg: " + res.getErrorMessage());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
