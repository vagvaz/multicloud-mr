import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.plugins.NutchTransformer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.cache.EnsembleCache;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.*;

//import org.apache.nutch.storage.WebPage;

/**
 * Created by vagvaz on 4/13/15.
 */
public class ReplayTool {
   private String baseDir;
   private int delay;
   private String webpagePrefixes;
   private String nutchDataPrefixes;
   private String ensembleString;
   private EnsembleCacheManager emanager;
   private EnsembleCache  nutchCache;
   private EnsembleCache webpageCache;
   private NutchTransformer nutchTransformer;
   public ReplayTool(String baseDir, String webpagePrefixes, String nutchDataPrefixes, String ensembleString, boolean multicloud){
      this.baseDir = baseDir;
      this.webpagePrefixes = webpagePrefixes;
      this.nutchDataPrefixes= nutchDataPrefixes;
      this.ensembleString = ensembleString;
      LQPConfiguration.initialize();
      emanager = new EnsembleCacheManager((ensembleString));
     emanager.start();
      nutchCache = emanager.getCache("WebPage",new ArrayList<>(emanager.sites()),
          EnsembleCacheManager.Consistency.DIST);
      if(multicloud)
         webpageCache = emanager.getCache("default.webpages",new ArrayList<>(emanager.sites()), EnsembleCacheManager.Consistency.DIST);
      else
         webpageCache = emanager.getCache("default.webpages",new ArrayList<>(emanager.sites()),
             EnsembleCacheManager.Consistency.DIST);



      List<String> mappings =    LQPConfiguration.getInstance().getConfiguration().getList("nutch.mappings");
      Map<String,String> nutchToLQE = new HashMap<String,String>();

      for(String mapping : mappings ){
         String[] keyValue = mapping.split(":");
         nutchToLQE.put(keyValue[0].trim(),keyValue[1].trim());
      }
      nutchTransformer = new NutchTransformer(nutchToLQE);
   }

   public void replayNutch(boolean load){
      int currentCounter = 0;
      long counter = 0;
      long nocontent_counter=0;
      Set<String> uniqueKeys = new HashSet<String>();
      while(true) {
         String[] prefixes = nutchDataPrefixes.split("\\|");

         for (String nutchDataPrefix : prefixes) {
            System.out.println("Checking Prefix... " +  nutchDataPrefix);
            try {

               File keyFile = new File(baseDir + "/" + nutchDataPrefix + "-" + currentCounter + ".keys");
               File dataFile = new File(baseDir + "/" + nutchDataPrefix + "-" + currentCounter + ".data");
               while (keyFile.exists() && dataFile.exists()) {
                  System.out.println("Exists_both... " +  nutchDataPrefix+ "-" + currentCounter + ".keys/data");
                  ObjectInputStream keyFileIS = new ObjectInputStream(new FileInputStream(keyFile));
                  ObjectInputStream dataFileIS = new ObjectInputStream(new FileInputStream(dataFile));
                  while (keyFileIS.available() > 0) {
                     int keysize = keyFileIS.readInt();
                     byte[] key = new byte[keysize];
                     keyFileIS.readFully(key);
                     int schemaSize = dataFileIS.readInt();
                     byte[] schemaBytes = new byte[schemaSize];
                     dataFileIS.readFully(schemaBytes);
                     String schemaJson = new String(schemaBytes);
                     Schema schema = new Schema.Parser().parse(schemaJson);
//
//                     // rebuild GenericData.Record
                     DatumReader<Object> reader = new GenericDatumReader<>(schema);
                     Decoder decoder = DecoderFactory.get().directBinaryDecoder(dataFileIS, null);
                     GenericData.Record page = new GenericData.Record(schema);
                     reader.read(page, decoder);
//
 //                    System.err.println("Read key: " + new String(key) + "\n" + "value " + page.toString());
//
                     if (load) {
                        Tuple t = nutchTransformer.transform(page);
                        if(t==null) {
                           System.err.print("Tuple is null!!!!");
                           nocontent_counter++;
                        }else {
                           if (t != null) {
                              if (page.get("content") != null) {
                                 if (!t.hasField("body")/*t.getAttribute("body") == null*/) {
                                    System.err.println("tuple does not have body field ");
                                    nocontent_counter++;
                                 }else {
                                    //System.err.print(" Put to cache");
                                    try {
                                       try {
                                         webpageCache.put(webpageCache.getName() + ":" + t.getAttribute("url"), t);
                                       } catch (org.infinispan.util.concurrent.TimeoutException | org.infinispan.client.hotrod.exceptions.HotRodClientException e) {
                                         // e.printStackTrace();
                                          delay*=1.2;
                                          System.out.println("Increasing delay x1.2, new delay " + delay + "ms");
                                       }

                                          Thread.sleep(delay);
                                       }  catch (Exception e) {
                                       System.out.println("another Exception Yeah");
                                       e.printStackTrace();

                                    }
                                    counter++;
                                    if(delay>10){
                                       System.err.print(" Inserted "+ counter);
                                    }
                                    uniqueKeys.add(t.getAttribute("url"));
                                    if (counter % 1000 == 0)
                                       System.err.println("loaded " + counter + " tuples");
                                 }
                              } else {
                                 nocontent_counter++;
                              }
                           } else {
                              System.err.println("tuple  is null ");
                              nocontent_counter++;
                           }
                        }

                     }
                  }
//                  else{
//                     System.err.println("File"+ baseDir + "/" + nutchDataPrefix + "-" + currentCounter + ".keys"+ " No available bytes ");
//                  }
                  currentCounter++;
                  keyFileIS.close();
                  dataFileIS.close();

                  keyFile = new File(baseDir + "/" + nutchDataPrefix + "-" + currentCounter + ".keys");
                  dataFile = new File(baseDir + "/" + nutchDataPrefix + "-" + currentCounter + ".data");

               }

               System.out.println("read " + currentCounter + " files and rejected " + nocontent_counter + ""
                                    + " for having null body" );
               currentCounter = 0;

            } catch (Exception e) {
               System.out.println("another Exception Yeah");
               e.printStackTrace();
               currentCounter++;
            }
         }
         System.out.println("Finally loaded: " + counter + " tuples, no content count: " + nocontent_counter + " unique " + uniqueKeys.size());
         break;
      }
   }
   public void setDelay(int delay) {
      this.delay = delay;
   }

}
