package tests;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * Created by Apostolos Nydriotis on 2015/07/10.
 */
public class SubmitKMeansTest {
  private static String host;
  private static int port;
  private static final String DRESDEN2_IP = "80.156.73.116";
  private static final String DD1A_IP = "80.156.222.4";
  //  private static final String HAMM5_IP = "5.147.254.161";
  private static final String HAMM6_IP = "5.147.254.199";
  private static final String CACHE_NAME = "clustered";
  private static final int PUT_THREADS_COUNT = 100;
  private static String ensembleString;
  private static Vector<File> files;
  private static String[] resultWords = {"to", "the", "of", "in", "on"};
  private static Map<String, Integer>[] centers;
  private static int k;

  private static class Putter implements Runnable {

    String id;
    long putCount;
    static int centerIndex;

    public Putter(int i) {
      id = String.valueOf(i);
      putCount = 0;
      centerIndex = k;
    }

    @Override
    public void run() {
      File f;

      while (true) {
        synchronized (files) {
          if (files.size() > 0) {
            f = files.get(0);
            files.remove(0);
            centerIndex--;
          } else {
            break;
          }
        }

        System.out.println(id + ": files.get(0).getAbsolutePath() = " + f.getAbsolutePath());

        EnsembleCacheManager ensembleCacheManager = new EnsembleCacheManager((ensembleString));

        EnsembleCache ensembleCache =
            ensembleCacheManager.getCache(CACHE_NAME,
                                          new ArrayList<>(ensembleCacheManager.sites()),
                                          EnsembleCacheManager.Consistency.DIST);
        try {
          BufferedReader bufferedReader =
              new BufferedReader(new InputStreamReader(new FileInputStream(f)));

          String line;
          Map<String, Integer> frequencies = new HashMap<>();

          while ((line = bufferedReader.readLine()) != null) {

            String[] words = line.split(" ");

            for (String word : words) {
              Integer wordFrequency = frequencies.get(word);
              if (wordFrequency == null) {
                frequencies.put(word, 1);
              } else {
                frequencies.put(word, wordFrequency + 1);
              }
            }
          }
          Tuple data = new Tuple();
          data.asBsonObject().putAll(frequencies);
          ensembleCache.put(id + "-" + String.valueOf(putCount++), new Tuple(data.toString()));

          if (centerIndex >= 0) {
            centers[centerIndex] = frequencies;
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

  public static void main(String[] args) {

    host = "http://" + DD1A_IP;  // dd1a
    port = 8080;
    String dataPath = ".";  // "/home/ap0n/Desktop/tmp-dataset";
    boolean loadData = false;
    boolean reduceLocal = false;
    k = 2;
    centers = new Map[k];

    if (args.length > 1) {
      host = args[0];
      port = Integer.parseInt(args[1]);
    }

    if (args.length > 2) {
      dataPath = args[2];
    }

    if (args.length > 3) {
      if (args[3].equals("true") || args[3].equals("false")) {
        loadData = Boolean.valueOf(args[3]);
      } else {
        PrintUsage();
        System.exit(-1);
      }
    }

    if (args.length > 4) {
      if (args[3].equals("true") || args[3].equals("false")) {
        reduceLocal = Boolean.valueOf(args[3]);
      } else {
        PrintUsage();
        System.exit(-1);
      }
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
    jsonObject.getObject("operator").putString("name", "kMeans");
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
//                       .putArray("hamm5", new JsonArray().add(HAMM5_IP))
                       .putArray("hamm6", new JsonArray().add(HAMM6_IP))
        )
        .putString("combine", "1")
        .putObject("targetEndpoints",
                   new JsonObject()
                       .putArray("dresden2", new JsonArray().add(DRESDEN2_IP))
                       .putArray("dd1a", new JsonArray().add(DD1A_IP))
//                       .putArray("hamm5", new JsonArray().add(HAMM5_IP))
                       .putArray("hamm6", new JsonArray().add(HAMM6_IP))
        );

    if (reduceLocal) {
      jsonObject.getObject("operator").putString("reduceLocal", "true");
    }

    try {

      ensembleString = DD1A_IP + ":11222" + "|"
                       + DRESDEN2_IP + ":11222" + "|"
//                     + HAMM5_IP + ":11222" + "|"
                       + HAMM6_IP + ":11222"
      ;

      if (loadData) {
        putData(dataPath);
      }

      while (true) {
        for (int i = 0; i < k; i++) {
          Map center = centers[i];
          jsonObject.getObject("operator")
              .getObject("configuration").putObject("center" + String.valueOf(i),
                                                    new JsonObject(center));
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

        // TODO(ap0n): read the results and compare the centers here.

        break;
      }
//      printResults(id, 5);
//      verifyResults(id, resultWords, ensembleString);
      printResults("metrics");
//
//      System.out.println("\nDONE IN: " + secs + " sec");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void verifyResults(String id, String[] resultWords, String ensembleString) {
    EnsembleCacheManager ensembleCacheManager = new EnsembleCacheManager(ensembleString);
    EnsembleCache cache = ensembleCacheManager.getCache(id, new ArrayList<>(
        ensembleCacheManager.sites()), EnsembleCacheManager.Consistency.DIST);
    for(String word : resultWords){
      Object result = cache.get(word);
      if(result != null) {
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

    System.out.println("dresden");
    remoteCacheManager = createRemoteCacheManager(DRESDEN2_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);

/*    System.out.println("hamm5");
    remoteCacheManager = createRemoteCacheManager(HAMM5_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);*/

    System.out.println("hamm6");
    remoteCacheManager = createRemoteCacheManager(HAMM6_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);
  }

  private static void printResults(String id,int numOfItems) {
    System.out.println("\n\ndd1a");
    RemoteCacheManager remoteCacheManager = createRemoteCacheManager(DD1A_IP);
    RemoteCache results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results,numOfItems);

    System.out.println("dresden");
    remoteCacheManager = createRemoteCacheManager(DRESDEN2_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results,numOfItems);

/*    System.out.println("hamm5");
    remoteCacheManager = createRemoteCacheManager(HAMM5_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results);*/

    System.out.println("hamm6");
    remoteCacheManager = createRemoteCacheManager(HAMM6_IP);
    results = remoteCacheManager.getCache(id);
    PrintUtilities.printMap(results,numOfItems);
  }

  private static void PrintUsage() {
    System.out.println("java -cp tests.SubmitWordCountTest http://<IP> <PORT> <DATA_DIR>"
                       + " <LOAD_DATA> <REDUCE_LOCAL>");
    System.out.println("Defaults:");
    System.out.println("java -cp tests.SubmitWordCountTest http://80.156.222.4 8080 . false false");
  }
}
