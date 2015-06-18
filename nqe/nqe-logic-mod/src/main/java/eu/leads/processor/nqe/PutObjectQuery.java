package eu.leads.processor.nqe;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Created by vagvaz on 8/4/14.
 */
@JsonAutoDetect
public class PutObjectQuery {
   String table;
   String key;
   String object;

   public String getTable() {
      return table;
   }

   public void setTable(String table) {
      this.table = table;
   }

   public String getKey() {
      return key;
   }

   public void setKey(String key) {
      this.key = key;
   }

   public String getObject() {
      return object;
   }

   public void setObject(String object) {
      this.object = object;
   }
   @Override
   public String toString(){
      return table+":"+key+ " -> \n->" + object;
   }
}
