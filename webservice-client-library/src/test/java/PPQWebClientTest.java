import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.web.WebServiceClient;

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.InvalidAlgorithmParameterException;
import java.util.List;

/**
 * Created by vagvaz on 10/29/14.
 */
public class PPQWebClientTest {
  private static String host;
  private static int port;

  public static void main(String[] args) {
    host = "http://localhost";
    //        host = "http://5.147.254.198";
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
    double k = 1.1;
    int N = 389032;
    int Svalue = 6000;
    int Bvalue = 10;
    int maximum_tuple_size = 120;
    try {
      WebServiceClient.encryptUpload(Svalue, k, "encryptedTest", "/home/vagvaz/sfile.key", "/home/vagvaz/Dataset.txt");
      List<String> result =
          WebServiceClient.getEncryptedData("leadsTest", "encryptedTest", "55574", "/home/vagvaz/sfile.key");
      PrintUtilities.printList(result);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InvalidAlgorithmParameterException e) {
      e.printStackTrace();
    }
  }
}
