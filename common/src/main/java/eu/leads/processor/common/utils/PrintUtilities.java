package eu.leads.processor.common.utils;

import org.slf4j.Logger;
import org.vertx.java.core.json.JsonObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 6/1/14.
 */
public class PrintUtilities {

    public static void printMap(Map<?, ?> map) {
        System.out.println("Map{\n");
        for (Object  e : map.keySet()) {
            System.out.println("\t " + e.toString() + "--->" + map.get(e).toString() + "\n");
        }
        System.out.println("end of map }");
    }

    public static void saveMapToFile(Map<?, ?> map,String filename) {
       RandomAccessFile raf = null;
       try {

          raf = new RandomAccessFile(filename,"rw");
       } catch (FileNotFoundException e) {
          e.printStackTrace();
       }
       System.out.println("Map{\n");
      for (Map.Entry<?, ?> e : map.entrySet()) {
         try {
            JsonObject val = new JsonObject((String) e.getValue());
            if(val.isObject())
            {

               raf.writeBytes(val.encodePrettily() + "\n");
            }
            else{
               raf.writeBytes("\t " + e.getKey().toString() + "--->" + e.getValue() + "\n");
            }
         } catch (IOException e1) {
            e1.printStackTrace();
         }
      }
       if (raf != null) {
          try {
             raf.close();
          } catch (IOException e) {
             e.printStackTrace();
          }
       }
       System.out.println("end of map }");
   }

    public static void printList(List<?> list) {
        System.err.println("List{");
        Iterator<?> it = list.iterator();
        while (it.hasNext()) {
            System.err.println("\t" + it.next().toString());
        }

        System.err.println("end of list}");
    }

    public static void printIterable(Iterator<Object> testCache) {
        System.out.println("Iterable{");
        Iterator<?> it = testCache;
        while (it.hasNext()) {
            System.out.println("\t" + it.next().toString());
        }
        System.out.println("end of iterable");
    }

    public static void logStackTrace(Logger profilerLog, StackTraceElement[] stackTrace) {
        for(StackTraceElement s : stackTrace){
            profilerLog.error(s.toString());
        }
    }
}
