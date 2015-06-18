package eu.leads.processor.core;

/**
 * Created by vagvaz on 9/24/14.
 */
public class TupleUtils {
   public static int compareValues(Object o1, Object o2, String type) {
      int result = 0;
      if(type.startsWith("TEXT")){
         String v1 = (String)o1;
         String v2 = (String)o2;
         result = v1.compareTo(v2);
      }
      else if (type.startsWith("BOOLEAN")){
         Boolean v1 = (Boolean)o1;
         Boolean v2 = (Boolean)o2;
         result = v1.compareTo(v2);
      }
      else if(type.startsWith("INT")){
         Number v1 = (Number)o1;
         Number v2 = (Number)o2;
         Long l1 = v1.longValue();
         Long l2 = v2.longValue();
         result = l1.compareTo(l2);
      }
      else if(type.startsWith("FLOAT") || type.startsWith("DOUBLE")){
         Number v1 = (Number)o1;
         Number v2 = (Number)o2;
         Double d1 = v1.doubleValue();
         Double d2 = v2.doubleValue();
         result = d1.compareTo(d2);
      }

      return result;
   }
}
