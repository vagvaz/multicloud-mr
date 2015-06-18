package data;

import eu.leads.processor.common.infinispan.AcceptAllFilter;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.conf.LQPConfiguration;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.util.CloseableIterable;
import org.vertx.java.core.json.JsonObject;

import java.io.*;
import java.util.Map;

/**
 * Created by vagvaz on 10/29/14.
 */
public class MySnapshot {
   static InfinispanManager inmanager;
   static RemoteCacheManager remoteCacheManager;
   public static void main(String[] args,InfinispanManager inmanager) throws IOException {
      if(args.length != 3 && args.length != 5 ){
         System.err.println("Wrong number of arguments action cacheName directory");
         System.err.println("                        [load/save]");
      }
      MySnapshot.inmanager = inmanager;

      System.in.read();
      if(args[0].toLowerCase().startsWith("load")){
         if(args.length == 3) {
            try {
               loadFrom(args[1], args[2]);
            } catch (IOException e) {
               e.printStackTrace();
            }
         }
         else{
            try{
               loadFromTo(args[1],args[2],args[3],args[4]);
            }catch (Exception e){
               e.printStackTrace();
            }
         }
      }
      else{
         try {
            saveTo(args[1],args[2]);
         } catch (IOException e) {
            e.printStackTrace();
         }
      }
   }

   private static RemoteCacheManager createRemoteCacheManager(String host, String port) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host(host).port(Integer.parseInt(port));
      return new RemoteCacheManager(builder.build());
   }

   private static void loadFromTo(String cacheName, String dir, String host, String port) throws IOException {
      if(checkDirectory(dir)){
         System.err.println("input is not a directory");
      }
      LQPConfiguration.initialize();
//      InfinispanManager manager = InfinispanClusterSingleton.getInstance().getManager();
      RemoteCacheManager manager = createRemoteCacheManager(host,port);
      RemoteCache cache =  manager.getCache(cacheName,true);
      BufferedReader keyReader = new BufferedReader(new InputStreamReader(new FileInputStream(dir+"/"+cacheName+".keys")));
//        BufferedReader sizeReader = new BufferedReader(new InputStreamReader(new FileInputStream(dir+"/"+cacheName+".sizes")));
      BufferedReader valueReader = new BufferedReader(new InputStreamReader(new FileInputStream(dir+"/"+cacheName+".values")));

      String keyLine = "";
      String valueLine = "";

      try {
         keyLine = keyReader.readLine();
         valueLine = valueReader.readLine();
      } catch (IOException e) {
         keyReader.close();
         valueReader.close();
         System.out.println("Emtpy files ?");
      }
      long counter = 0;
      try {
         while (true && keyLine != null){
            if(keyLine != null && !keyLine.trim().equals("")){
               if(valueLine != null && !valueLine.trim().equals("")) {
                  JsonObject ob = new JsonObject(valueLine);
                  cache.put(keyLine.trim(), valueLine.trim());
               }
            }

            System.out.println(counter++);
            keyLine = keyReader.readLine();
            valueLine = valueReader.readLine();
         }
      }catch(IOException e){
         keyReader.close();
         valueReader.close();
         System.out.println("Read " + counter + "tuples");
      }
   }

   private static void saveTo(String arg, String arg1) throws IOException {
      if(!checkDirectory(arg1)){
         System.err.println("input is not a directory");
      }
      String cacheName = arg;
      LQPConfiguration.initialize();
      InfinispanManager manager = InfinispanClusterSingleton.getInstance().getManager();
      Cache cache = (Cache) manager.getPersisentCache(cacheName);
      if(cache.size() == 0){
         System.out.println("cache size is 0");
         return;
      }
      FileWriter keyOut = new FileWriter(arg1+"/"+arg+".keys");
      FileWriter sizeOut = new FileWriter(arg1+"/"+arg+".sizes");
      FileWriter valueOut = new FileWriter(arg1+"/"+arg+".values");


      FileWriter writer;

      CloseableIterable<Map.Entry<String, String>> iterable =
              cache.getAdvancedCache().filterEntries(new AcceptAllFilter());
      long counter = 0;
      for (Map.Entry<String, String> entry : iterable) {
         keyOut.write(entry.getKey()+"\n");
         valueOut.write(entry.getValue()+"\n");
         System.out.println(counter++);
      }
      keyOut.close();
      valueOut.close();
      System.out.println("Stored " + counter + " tuples ");

   }

   private static boolean checkDirectory(String arg1) {
      File file = new File(arg1);
      return file.isDirectory();
   }

   private static void loadFrom(String cacheName, String dir) throws IOException {
      if(checkDirectory(dir)){
         System.err.println("input is not a directory");
      }
      LQPConfiguration.initialize();
      InfinispanManager manager = InfinispanClusterSingleton.getInstance().getManager();
      Cache cache = (Cache) manager.getPersisentCache(cacheName);
      BufferedReader keyReader = new BufferedReader(new InputStreamReader(new FileInputStream(dir+"/"+cacheName+".keys")));
//        BufferedReader sizeReader = new BufferedReader(new InputStreamReader(new FileInputStream(dir+"/"+cacheName+".sizes")));
      BufferedReader valueReader = new BufferedReader(new InputStreamReader(new FileInputStream(dir+"/"+cacheName+".values")));



      String keyLine = "";
      String valueLine = "";

      try {
         keyLine = keyReader.readLine();
         valueLine = valueReader.readLine();
      } catch (IOException e) {
         keyReader.close();
         valueReader.close();
         System.out.println("Emtpy files ?");
      }
      long counter = 0;
      try {
         while (true && keyLine != null){
            if(keyLine != null && !keyLine.trim().equals("")){
               if(valueLine != null && !valueLine.trim().equals("")) {
                  JsonObject ob = new JsonObject(valueLine);
                  cache.put(keyLine.trim(), valueLine.trim());
                  System.out.println(counter++);
               }
            }

            counter++;
            keyLine = keyReader.readLine();
            valueLine = valueReader.readLine();
         }
      }catch(IOException e){
         keyReader.close();
         valueReader.close();
         System.out.println("Read " + counter + "tuples");
      }

   }
}
