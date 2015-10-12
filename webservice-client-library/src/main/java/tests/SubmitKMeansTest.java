package tests;

import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.web.QueryStatus;
import eu.leads.processor.web.WebServiceClient;
import org.bson.BasicBSONObject;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.Site;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.*;
import java.net.MalformedURLException;
import java.util.*;

/**
 * Created by Apostolos Nydriotis on 2015/07/10.
 */
public class SubmitKMeansTest {

  private static final String DRESDEN2_IP = "80.156.73.116";
  private static final String DD1A_IP = "80.156.222.4";
  private static final String DD2A_IP = "87.190.238.119";
  private static final String HAMM5_IP = "5.147.254.161";
  private static final String HAMM6_IP = "5.147.254.199";
  private static final String CACHE_NAME = "clustered";
  private static final int PUT_THREADS_COUNT = 100;
  private static String host;
  private static int port;
  private static Map<String, String> microcloudAddresses;
  private static Map<String, String> activeIps;
  private static List<String> activeMicroClouds;
  private static String ensembleString;
  private static Vector<File> files;
  private static String[] resultWords = {"to", "the", "of", "in", "on"};
  private static Map<String, Double>[] centroids;
  private static Double[] norms;
  private static int k;

  private static void putData(String dataDirectory) {

    File datasetDirectory = new File(dataDirectory);
    File[] allFiles = datasetDirectory.listFiles();
    files = new Vector<File>();

    for (File f : allFiles) {
      files.add(f);
    }

    Vector<Thread> threads = new Vector<>(PUT_THREADS_COUNT);

    for (int i = 0; i < PUT_THREADS_COUNT; i++) {
      threads.add(new Thread(new Putter(i)));
    }

    System.out.print("Loading data to '" + CACHE_NAME + "' cache\n ");

    for (Thread t : threads) {
      t.start();
    }

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private static Object getKeyFrom(EnsembleCache ensembleCache, Object key) {
    Object result = null;
    for (Object s : ensembleCache.sites()) {
      EnsembleCache siteCache = ((Site) s).getCache(ensembleCache.getName());
      result = siteCache.get(key);
      if (result != null) {
        return result;
      }
    }
    return result;
  }

  public static void main(String[] args) {

    host = "http://" + DD1A_IP;  // dd1a
    port = 8080;

    String propertiesFile = "client.properties";
    if (args.length != 1) {
      PrintUsage();
    } else {
      propertiesFile = args[0];
    }

    LQPConfiguration.getInstance().initialize();
    LQPConfiguration.getInstance().loadFile(propertiesFile);
    host = LQPConfiguration.getInstance().getConfiguration().getString("webservice-address", "http://" + DD1A_IP);
    System.out.println("webservice host: " + host);
    port = 8080;
    String dataPath = LQPConfiguration.getInstance().getConfiguration().getString("data-path", ".");
    System.out.println("data path " + dataPath);
    boolean loadData = LQPConfiguration.getInstance().getConfiguration().getBoolean("load-data", false);
    System.out.println("load data " + loadData);

    boolean reduceLocal = LQPConfiguration.getInstance().getConfiguration().getBoolean("use-reduce-local", false);
    System.out.println("use reduce local " + reduceLocal);

    boolean combine = LQPConfiguration.getInstance().getConfiguration().getBoolean("use-combine", true);
    System.out.println("use combine " + combine);

    k = LQPConfiguration.getInstance().getConfiguration().getInt("k", 2);
    System.out.println("k " + k);
    centroids = new Map[k];
    norms = new Double[k];

    //set the default microclouds
    List<String> defaultMCs = new ArrayList<>(Arrays.asList("dd1a", "dd2a", "dresden2"));
    //read the microcloud to run the job
    activeMicroClouds = LQPConfiguration.getInstance().getConfiguration().getList("active-microclouds", defaultMCs);
    System.out.println("active mc ");
    PrintUtilities.printList(activeMicroClouds);
    //initialize default values
    microcloudAddresses = new HashMap<>();
    microcloudAddresses.put("dd1a", DD1A_IP);
    microcloudAddresses.put("dd2a", DD2A_IP);
    microcloudAddresses.put("dresden2", DRESDEN2_IP);
    microcloudAddresses.put("hamm6", HAMM6_IP);
    microcloudAddresses.put("hamm5", HAMM5_IP);

    activeIps = new HashMap<>();
    //read the ips from configuration or use the default
    for (String mc : activeMicroClouds) {
      activeIps.put(mc, LQPConfiguration.getInstance().getConfiguration().getString(mc, microcloudAddresses.get(mc)));
    }
    System.out.println("active ips");
    PrintUtilities.printMap(activeIps);

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
    jsonObject.getObject("operator").putString("name", "kMeans");
    jsonObject.getObject("operator").putArray("inputs", new JsonArray().add(CACHE_NAME));
    jsonObject.getObject("operator").putString("output", "testOutputCache");

    JsonObject scheduling = getScheduling(activeMicroClouds, activeIps);
    jsonObject.getObject("operator").putObject("scheduling", scheduling);

    if (combine) {
      jsonObject.getObject("operator").putString("combine", "1");
    }

    if (reduceLocal) {
      jsonObject.getObject("operator").putString("reduceLocal", "true");
    }

    JsonObject targetEndpoints = scheduling;
    jsonObject.getObject("operator").putObject("targetEndpoints", targetEndpoints);

    jsonObject.getObject("operator").getObject("configuration").putNumber("k", k);

    try {
      ensembleString = "";
      for (String mc : activeMicroClouds) {
        ensembleString += activeIps.get(mc) + ":11222|";
      }
      //      ensembleString += "80.156.222.26" + ":11222|" + "87.190.238.120" + ":11222|";
      ensembleString = ensembleString.substring(0, ensembleString.length() - 1);

      if (loadData) {
        putData(dataPath);
      }

      Date start = new Date();
      String[] clusters = new String[k];
      while (true) {
        for (int i = 0; i < k; i++) {
          Map centroid = centroids[i];
          jsonObject.getObject("operator").getObject("configuration")
              .putObject("centroid" + String.valueOf(i), new JsonObject(centroid))
              .putNumber("norm" + String.valueOf(i), norms[i]);
        }

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

        EnsembleCacheManager ensembleCacheManager = new EnsembleCacheManager(ensembleString);
        EnsembleCache cache = ensembleCacheManager
            .getCache(id, new ArrayList<>(ensembleCacheManager.sites()), EnsembleCacheManager.Consistency.DIST);

        Map<String, Double>[] newCenters = new Map[k];
        for (int i = 0; i < k; i++) {
          //          Tuple t = (Tuple) cache.get(String.valueOf(i));
          Tuple t = (Tuple) getKeyFrom(cache, String.valueOf(i));
          norms[i] = t.getNumberAttribute("norm" + String.valueOf(i)).doubleValue();
          clusters[i] = t.getAttribute("cluster" + i);
          Tuple valueTuple = new Tuple((BasicBSONObject) t.getGenericAttribute("newCentroid"));
          newCenters[i] = new HashMap<>();
          for (String key : valueTuple.getFieldNames()) {
            newCenters[i].put(key, valueTuple.getNumberAttribute(key).doubleValue());
          }
        }

        if (!centersChanged(newCenters)) {
          System.out.println();
          break;
        }
        for (int i = 0; i < k; i++) {
          centroids[i] = newCenters[i];
        }
        System.out.println("\nRecalculating");
      }

      for (int i = 0; i < centroids.length; i++) {
        System.out.println("cluster" + i + ": " + clusters[i]);
      }

      //        printResults(id, 5);
      Date end = new Date();
      System.out.println("\nDONE IN: " + ((double) (end.getTime() - start.getTime()) / 1000.0) + " sec");

      //      printResults("metrics");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static boolean centersChanged(Map<String, Double>[] newCenters) {
    for (int i = 0; i < k; i++) {
      Map<String, Double> newCenter = newCenters[i];
      Map<String, Double> oldCenter = centroids[i];
      if (newCenter.size() != oldCenter.size()) {
        return true;
      }

      for (Map.Entry<String, Double> entry : newCenter.entrySet()) {
        Double oldValue = oldCenter.get(entry.getKey());
        if (oldValue == null || oldValue.intValue() != entry.getValue().intValue()) {
          return true;
        }
      }
    }
    return false;
  }

  private static JsonObject getScheduling(List<String> activeMicroClouds, Map<String, String> activeIps) {
    JsonObject result = new JsonObject();
    for (String mc : activeMicroClouds) {
      result.putArray(mc, new JsonArray().add(activeIps.get(mc)));
    }
    return result;
  }

  private static void verifyResults(String id, String[] resultWords, String ensembleString) {
    EnsembleCacheManager ensembleCacheManager = new EnsembleCacheManager(ensembleString);
    EnsembleCache cache = ensembleCacheManager
        .getCache(id, new ArrayList<>(ensembleCacheManager.sites()), EnsembleCacheManager.Consistency.DIST);
    for (String word : resultWords) {
      Object result = cache.get(word);
      if (result != null) {
        System.out.println(word + "--->" + result.toString());
      } else {
        System.out.println(word + " NULL");
      }
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

    System.out.println("\n\ndd2a");
    remoteCacheManager = createRemoteCacheManager(DD2A_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);
    //
    //    System.out.println("dresden");
    //    remoteCacheManager = createRemoteCacheManager(DRESDEN2_IP);
    //    results = remoteCacheManager.getCache(id);
    //    PrintUtilities.printMap(results);

/*    System.out.println("hamm5");
    remoteCacheManager = createRemoteCacheManager(HAMM5_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);*/

    //    System.out.println("hamm6");
    //    remoteCacheManager = createRemoteCacheManager(HAMM6_IP);
    //    results = remoteCacheManager.getCache(id);
    //    PrintUtilities.printMap(results);

    //    System.out.println("\n\nlocalcluster");
    //    RemoteCacheManager remoteCacheManager = createRemoteCacheManager("192.168.178.4");
    //    RemoteCache results = remoteCacheManager.getCache(id);
    //    PrintUtilities.printMap(results);

  }

  private static void printResults(String id, int numOfItems) {
    System.out.println("\n\ndd1a");
    RemoteCacheManager remoteCacheManager = createRemoteCacheManager(DD1A_IP);
    RemoteCache results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results, numOfItems);

    System.out.println("\n\ndd2a");
    remoteCacheManager = createRemoteCacheManager(DD2A_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results, numOfItems);
    //
    //    System.out.println("dresden");
    //    remoteCacheManager = createRemoteCacheManager(DRESDEN2_IP);
    //    results = remoteCacheManager.getCache(id);
    //    PrintUtilities.printMap(results, numOfItems);

/*    System.out.println("hamm5");
    remoteCacheManager = createRemoteCacheManager(HAMM5_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);*/

    //    System.out.println("hamm6");
    //    remoteCacheManager = createRemoteCacheManager(HAMM6_IP);
    //    results = remoteCacheManager.getCache(id);
    //    PrintUtilities.printMap(results, numOfItems);

    //    System.out.println("\n\nlocalcluster");
    //    RemoteCacheManager remoteCacheManager = createRemoteCacheManager("192.168.178.4");
    //    RemoteCache results = remoteCacheManager.getCache(id);
    //    PrintUtilities.printMap(results, numOfItems);
  }

  private static void PrintUsage() {
    System.out
        .println("java -cp tests.SubmitWordCountTest http://<IP> <PORT> <DATA_DIR>" + " <LOAD_DATA> <REDUCE_LOCAL>");
    System.out.println("Defaults:");
    System.out.println("java -cp tests.SubmitWordCountTest http://80.156.222.4 8080 . false false");
  }

  private static class Putter implements Runnable {

    static int centerIndex;
    int id;
    long putCount;
    int fileIsCenterIndex;

    public Putter(int i) {
      id = i;
      putCount = 0;
      centerIndex = k;
      fileIsCenterIndex = -1;
    }

    @Override public void run() {
      File f;

      EnsembleCacheManager ensembleCacheManager = new EnsembleCacheManager((ensembleString));

      EnsembleCache ensembleCache = ensembleCacheManager
          .getCache(CACHE_NAME, new ArrayList<>(ensembleCacheManager.sites()), EnsembleCacheManager.Consistency.DIST);
      while (true) {
        synchronized (files) {
          if (files.size() > 0) {
            f = files.remove(0);
            fileIsCenterIndex = --centerIndex;
          } else {
            break;
          }
        }

        System.out.println(id + ": files.get(0).getAbsolutePath() = " + f.getAbsolutePath());

        try {
          BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));

          String line;
          Map<String, Double> frequencies = new HashMap<>();

          while ((line = bufferedReader.readLine()) != null) {

            String[] words = line.split(" ");

            for (String word : words) {
              if (word.length() == 0) {
                continue;
              }
              Double wordFrequency = frequencies.get(word);
              if (wordFrequency == null) {
                frequencies.put(word, 1d);
              } else {
                frequencies.put(word, wordFrequency + 1);
              }
            }
          }
          frequencies.put("~", Double.valueOf(f.getName().hashCode()));
          Tuple data = new Tuple();
          data.asBsonObject().putAll(frequencies);
          //          System.out.println("Putting size " + frequencies.size());
          //          System.out.println("Putting id " + frequencies.get("~"));
          ensembleCache.put(String.valueOf(id) + "-" + String.valueOf(putCount++), data);

          if (fileIsCenterIndex >= 0) {
            centroids[fileIsCenterIndex] = frequencies;
            Double norm = 0d;
            for (Map.Entry<String, Double> e : frequencies.entrySet()) {
              if (e.getKey().equals("~")) {
                continue;
              }
              norm += e.getValue() * e.getValue();
            }
            norms[fileIsCenterIndex] = new Double(norm);
          }

          bufferedReader.close();
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
