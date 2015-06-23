import eu.leads.processor.web.ActionResult;
import eu.leads.processor.web.WebServiceClient;

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
    jsonObject.putString("name", "wordCount");

    try {
      ActionResult res = WebServiceClient.executeMapReduceJob(jsonObject, host + ":" + port);
      System.out.println(res.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
