package eu.leads.processor.core.netty;

import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.conf.ConfigurationUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.core.WebUtils;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.vertx.java.core.json.JsonObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by vagvaz on 11/26/15.
 */
public class NettyNodeTest {
  private static Tuple t;

  public static void main(String[] args) throws IOException {
    LQPConfiguration.initialize();
    LQPConfiguration.getInstance().getConfiguration().setProperty("node.current.component", "nettyNodeTest " + args[0]);
    RandomAccessFile raf = new RandomAccessFile(args[1], "rw");
    long size = raf.length();
    byte[] bytes = new byte[(int) size];
    raf.read(bytes);
    String json = new String(bytes);
    JsonObject conf = new JsonObject(json);
    JsonObject globalConfig = conf.getObject("global");

    String publicIP = ConfigurationUtilities
        .getPublicIPFromGlobal(LQPConfiguration.getInstance().getMicroClusterName(), globalConfig);
    LQPConfiguration.getInstance().getConfiguration().setProperty(StringConstants.PUBLIC_IP, publicIP);

    InfinispanManager manager = InfinispanClusterSingleton.getInstance().getManager();
    /**
     * !!NETTY new code for the new
     */
    IndexManager.initialize(new Properties());
    NettyDataTransport.initialize(globalConfig);
    /**
     * END OF CODE
     */

    String ensemble = WebUtils.getEnsembleString(globalConfig);
    EnsembleCacheManager emanager = new EnsembleCacheManager(ensemble);
    NettyKeyValueDataTransfer keyValueDataTransfer = new NettyKeyValueDataTransfer();
    keyValueDataTransfer.initialize(emanager);

    int numberofkeys = 500000;
    int numberofvalues = 2;
    String baseKey = "baseKeyString";

    long start = System.nanoTime();
    ArrayList<String> tuples = generate(numberofkeys, numberofvalues);
    System.out.println("insert");
    BasicCache cache = emanager.getCache("clustered");
    for (String k : tuples) {
      //            System.out.println("key " + key);
      //            for(int value =0; value < numberofvalues; value++){
      keyValueDataTransfer.putToCache(cache, k, t);
      //            }
    }
    try {
      keyValueDataTransfer.waitForAllPuts();
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    } catch (ExecutionException e1) {
      e1.printStackTrace();
    }
    long end = System.nanoTime();
    long dur = end - start;
    dur /= 1000000;
    int total = numberofkeys * numberofvalues;
    double avg = total / (double) dur;

    System.out.println("Put " + (total) + " in " + (dur) + " avg " + avg);

    System.out.println("exit---");


    System.err.println("Bye bye after pressing any button");
    try

    {
      System.in.read();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  private static ArrayList<String> generate(int numberofkeys, int numberofvalues) {
    String baseKey = "baseKeyString";
    System.out.println("generate");
    ArrayList<String> result = new ArrayList<>(numberofkeys * numberofvalues);
    for (int key = 0; key < numberofkeys; key++) {
      //            System.out.println("key " + key);
      for (int value = 0; value < numberofvalues; value++) {
        result.add(baseKey + key);
      }
    }
    Collections.shuffle(result);
    return result;
  }

  private static void initTuple() {
    t = new Tuple();
    int key = 4;
    int value = 5;
    for (int i = 0; i < 4; i++) {
      t.setAttribute("key-" + key + "-" + i, key);
      t.setAttribute("value-" + value + "-" + i, value);
      t.setAttribute("keyvalue-" + key + "." + value + "-" + i, key * value);
    }
  }

  private static Tuple getTuple(int key, int value) {

    //        t = new Tuple();
    //        int key = 4;
    //        int value = 5;
    //        t.setAttribute("key","baseKey"+Integer.toString(key));
    //        t.setAttribute("value",Integer.toString(value));
    //        for(int i = 0 ; i < 4; i++){
    //            t.setAttribute("key-"+key+"-"+i,key);
    //            t.setAttribute("value-"+value+"-"+i,value);
    //            t.setAttribute("keyvalue-"+key+"."+value+"-"+i,key*value);
    //        }
    return t;
  }
}


