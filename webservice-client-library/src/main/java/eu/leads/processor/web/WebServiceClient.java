package eu.leads.processor.web;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.PlatformManager;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.ws.rs.core.MediaType;

/**
 * Created by vagvaz on 8/15/14.
 */
public class WebServiceClient {

  private static PlatformManager pm;
  private final static String prefix = "/rest/";
  private final static ObjectMapper mapper = new ObjectMapper();
  private static String host;
  private static String port;
  private static URL address;
  private static Vertx vertx;
  private static PlatformManager platformManager;
  HttpClient httpClient;

  public static boolean initialize(String url, int p) throws MalformedURLException {
    host = url;
    port = String.valueOf(p);
    address = new URL(host + ":" + port);
    return true;
  }

  public static boolean initialize(String uri) throws MalformedURLException {
    int lastIndex = uri.lastIndexOf(":");
    host = uri.substring(0, lastIndex);
    port = uri.substring(lastIndex + 1);
    address = new URL(host + ":" + port);
    return true;
  }

  public static boolean checkIfOnline() {
    HttpURLConnection connection = null;
    try {
      address = new URL(host + ":" + port + prefix + "checkOnline");
      connection = (HttpURLConnection) address.openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setUseCaches(false);
      connection.setDoInput(true);
      connection.setDoOutput(true);
      InputStream is = connection.getInputStream();
      BufferedReader rd = new BufferedReader(new InputStreamReader(is));
      StringBuffer response = new StringBuffer();
      String line;
      while ((line = rd.readLine()) != null) {
        response.append(line);
      }
      rd.close();
      return true;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static HttpURLConnection setUp(HttpURLConnection connection, String type,
                                         String contentType, boolean hasInput,
                                         boolean hasOutput) throws ProtocolException {
    connection.setRequestMethod(type);
    connection.setRequestProperty("Content-Type", contentType);
    connection.setUseCaches(false);
    connection.setDoInput(hasInput);
    connection.setDoOutput(hasOutput);
    connection.setConnectTimeout(4000);
    //connection.setReadTimeout(10000);
    return connection;
  }

  private static String getResult(HttpURLConnection connection) throws IOException {
    //        System.out.println("getResult");
    InputStream is = connection.getInputStream();
    BufferedReader rd = new BufferedReader(new InputStreamReader(is));
    StringBuffer response = new StringBuffer();
    String line;
    while ((line = rd.readLine()) != null) {
      response.append(line);
    }
    rd.close();
    response.trimToSize();
    //        System.out.println("received: " + response);
    return response.toString();
  }

  private static void setBody(HttpURLConnection connection, Object body) throws IOException {
    String output = mapper.writeValueAsString(body);
    //        System.out.println("Size: " + output.getBytes().length);
    DataOutputStream os = new DataOutputStream(connection.getOutputStream());
    os.writeBytes(output);
    os.flush();
    os.close();
  }

  private static void setBody(HttpURLConnection connection, JsonObject body) throws IOException {
    String output = body.toString();
    //        System.out.println("Size: " + output.getBytes().length);
    DataOutputStream os = new DataOutputStream(connection.getOutputStream());
    os.writeBytes(output);
    os.flush();
    os.close();
  }

  private static void setDataBody(HttpURLConnection connection, byte[] data) throws IOException {
    //String output = mapper.writeValueAsString(body);
    // System.out.println("Size: " + output.getBytes().length);
    DataOutputStream os = new DataOutputStream(connection.getOutputStream());
    //byte[] data = SerializationUtils.serialize(body);
    System.out.println("setDataBody length: " + data.length);

    os.write(data, 0, data.length);

    os.flush();
    os.close();
  }

  public static ActionResult executeMapReduceJob(JsonObject job, String uri) throws IOException {
    address = new URL(uri + "/rest/mrjob/submit/");
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    //  TODO(ap0n): Check the last 2 args of setUp
    connection = setUp(connection, "POST", MediaType.APPLICATION_JSON, true, true);
    setBody(connection, job);
    String response = getResult(connection);
    ActionResult result = mapper.readValue(response, ActionResult.class);
    return result;  // TODO(ap0n): is this necessary? //yes it should contain the ID of the job
  }

  public static ActionResult executeMapReduce(JsonObject newAction, String uri) throws IOException {
    address = new URL(uri + "/rest/internal/executemr");
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    connection = setUp(connection, "POST", MediaType.APPLICATION_JSON, true, true);
    setBody(connection, newAction);
    String response = getResult(connection);
    ActionResult result = mapper.readValue(response, ActionResult.class);
    return result;
  }

  public static ActionResult executeMapReduce(JsonObject mrAction, String host, String port)
      throws IOException {
    address = new URL(host + ":" + port + prefix + "internal/executemr");
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    connection = setUp(connection, "POST", MediaType.APPLICATION_JSON, true, true);
    setBody(connection, mrAction);
    String response = getResult(connection);
    ActionResult result = mapper.readValue(response, ActionResult.class);
    return result;
  }

  public static ActionResult completeMapReduce(JsonObject mrAction, String uri) throws IOException {
    address = new URL(uri + "/" + prefix + "internal/completedmr");
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    connection = setUp(connection, "POST", MediaType.APPLICATION_JSON, true, true);
    setBody(connection, mrAction);
    String response = getResult(connection);
    ActionResult result = mapper.readValue(response, ActionResult.class);
    return result;
  }

  public static JsonObject getObject(String table, String key, List<String> attributes)
      throws IOException {

    ObjectQuery ob = new ObjectQuery();
    ob.setAttributes(attributes);
    ob.setKey(key);
    ob.setTable(table);
    String atr = "";
    ob.setAttributes(attributes);
    address = new URL(host + ":" + port + prefix + "object/get/");
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    connection = setUp(connection, "POST", MediaType.APPLICATION_JSON, true, true);
    setBody(connection, ob);
    String response = getResult(connection);
    //        System.out.println("getResponse " + response);
    if (response.length() < 5) {
      return null;
    }
    //      HashMap<String,String> res = (HashMap<String, String>) mapper.readValue(response, HashMap.class);
    //      HashMap<String,String> result = new HashMap<>();
    //        for(Map.Entry<String,String> r : res.entrySet()){
    ////            if(!r.getValue().startsWith("["))
    ////               result.put(r.getKey(),mapper.readValue(r.getValue(),String.class));
    ////            else
    //               result.put(r.getKey(),r.getValue());
    //        }
    JsonObject result = new JsonObject(response);
    return result;
  }

  public static boolean putObject(String table, String key, JsonObject object)
      throws IOException {
    boolean result = false;
    PutAction action = new PutAction();
    action.setTable(table);
    action.setKey(key);
    action.setObject(object.toString());
    address = new URL(host + ":" + port + prefix + "object/put/");
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    connection = setUp(connection, "POST", MediaType.APPLICATION_JSON, true, true);
    //        setBody(connection,mapper.writeValueAsString(action));
    setBody(connection, action);
    String response = getResult(connection);
    ActionResult aresult = mapper.readValue(response, ActionResult.class);
    result = aresult.getStatus().equals("SUCCESS");
    return result;
  }

  // TODO(ap0n): Will help for the result
  public static QueryStatus getQueryStatus(String id) throws IOException {
    QueryStatus result = new QueryStatus();
    address = new URL(host + ":" + port + prefix + "query/status/" + id);
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    connection = setUp(connection, "GET", MediaType.APPLICATION_JSON, true, true);
    String response = getResult(connection);
    //System.err.println("responsed " + response);
    //System.out.print(". ");
    result = mapper.readValue(response, QueryStatus.class);
    return result;
  }

  public static QueryResults getQueryResults(String id, long min, long max) throws IOException {
    QueryResults result = new QueryResults();
    address = new URL(host + ":" + port + prefix + "query/results/" + id + "/min/" + String
        .valueOf(min)
                      + "/max/" + String.valueOf(max));
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    connection = setUp(connection, "GET", MediaType.APPLICATION_JSON, true, true);
    String response = getResult(connection);
    result = new QueryResults(new JsonObject(response));
    return result;
  }

  public static QueryStatus submitQuery(String username, String SQL) throws IOException {
    QueryStatus result = null;
    WebServiceQuery query = new WebServiceQuery();
    query.setSql(SQL);
    query.setUser(username);
    address = new URL(host + ":" + port + prefix + "query/submit");
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    connection = setUp(connection, "POST", MediaType.APPLICATION_JSON, true, true);
    setBody(connection, query);
    String response = getResult(connection);
    result = mapper.readValue(response, QueryStatus.class);
    return result;
  }

  public static QueryStatus submitWorkflow(String username, String workflow) throws IOException {
    QueryStatus result = null;
    WebServiceWorkflow query = new WebServiceWorkflow();
    query.setWorkflow(workflow);
    query.setUser(username);
    address = new URL(host + ":" + port + prefix + "workflow/submit");
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    connection = setUp(connection, "POST", MediaType.APPLICATION_JSON, true, true);
    setBody(connection, query);
    String response = getResult(connection);
    result = mapper.readValue(response, QueryStatus.class);
    return result;
  }

  public static boolean uploadJar(String username, String jarPath, String prefix, int chunkSize) {
    try {
      long StartTime = System.currentTimeMillis();
      long totalUploadTime = 0;
      System.out.println("UploadJar chunks size: " + chunkSize);
      BufferedInputStream input = new BufferedInputStream(new FileInputStream(jarPath));
      ByteArrayOutputStream array = new ByteArrayOutputStream();
      byte[] buffer = new byte[chunkSize];
      byte[] toWrite = null;
      int size = input.available();
      int initialSize = size;
      int partsNum = size / chunkSize + 1;
      System.out.println("Must upload " + size + " bytes, in " + partsNum + " parts.");
      int counter = -1;
      float currentSpeed = 0;
      while (size > 0) {
        counter++;

        int readSize = input.read(buffer);
        toWrite = Arrays.copyOfRange(buffer, 0, readSize);
        if (!uploadData(username, toWrite, prefix + "/" + counter)) {
          return false;
        }

        long endTime = System.currentTimeMillis();
        long timeDiff = endTime - StartTime + 1;
        StartTime = endTime;
        currentSpeed = (chunkSize / 1000f) / ((timeDiff + 1f) / 1000f);
        totalUploadTime += timeDiff;
        size = input.available();
        long ET = (int) (size / (chunkSize / (timeDiff + 1)));

        System.out.println(
            "Uploaded chunk #" + (counter + 1) + "/" + partsNum + ", speed:  " + currentSpeed
            + " kb/s, " + size + " bytes to go estimated finish in:  " + ConvertSecondToHHMMString(
                ET));
      }
      currentSpeed = (initialSize / 1000f) / (totalUploadTime / 1000f);
      System.out.println(
          "Upload Completed in: " + ConvertSecondToHHMMString(totalUploadTime) + " Avg Speed: "
          + currentSpeed + " kb/s, ");
      return true;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  private static String ConvertSecondToHHMMString(long millisecondtTime) {
    TimeZone tz = TimeZone.getTimeZone("UTC");
    SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
    df.setTimeZone(tz);
    String time = df.format(new Date(millisecondtTime));

    return time;

  }

  public static boolean uploadData(String username, byte[] data, String target) {
    boolean result = false;
    try {
      address = new URL(host + ":" + port + prefix + "data/upload/");
      JsonObject action = new JsonObject();
      HttpURLConnection connection = (HttpURLConnection) address.openConnection();
      connection = setUp(connection, "POST", MediaType.APPLICATION_JSON, true, true);
      //        setBody(connection,mapper.writeValueAsString(action));
      action.putBinary("data", data);
      action.putString("path", target);
      action.putString("user", username);
      setBody(connection, action);
      String response = getResult(connection);
      ActionResult aresult = mapper.readValue(response, ActionResult.class);
      result = aresult.getStatus().equals("SUCCESS");
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (ProtocolException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }


  private static boolean waitForFinish(JsonObject reply) throws IOException {
    String queryId = reply.getString("id");
    QueryStatus status = WebServiceClient.getQueryStatus(queryId);
    while (!status.getStatus().equals("COMPLETED") && !status.getStatus().equals("FAILED")) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      status = WebServiceClient.getQueryStatus(status.getId());
    }
    return status.getStatus().equals("COMPLETED");
  }

  private static JsonObject submitEncryptedQuery(String user, String encryptedCache, String token)
      throws IOException {
    JsonObject result = new JsonObject();

    JsonObject encryptedQuery = new JsonObject();
    encryptedQuery.putString("token", token);
    encryptedQuery.putString("cache", encryptedCache);
    encryptedQuery.putString("user", user);
    address = new URL(host + ":" + port + prefix + "query/encrypted/ppq");
    HttpURLConnection connection = (HttpURLConnection) address.openConnection();
    connection = setUp(connection, "POST", MediaType.APPLICATION_JSON, true, true);
    setBody(connection, encryptedQuery);
    String response = getResult(connection);
    JsonObject reply = new JsonObject(response);
    //            result.put("id",reply.getString("id"));
    //            result.put("output",reply.getString("output"));
    result = reply;

    return result;
  }

}
