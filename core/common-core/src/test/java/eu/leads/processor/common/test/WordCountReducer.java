package eu.leads.processor.common.test;


import eu.leads.processor.infinispan.LeadsReducer;

import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class WordCountReducer extends LeadsReducer<String, Integer> {

  private static final long serialVersionUID = 1901016598354633256L;

  public WordCountReducer(JsonObject configuration) {
    super(configuration);

  }

  public Integer reduce(String key, Iterator<Integer> iter) {
    int sum = 0;
    try {
      while (iter.hasNext()) {
        Integer i = iter.next();
        sum += i;
      }
    }catch (Exception e){
      if(e instanceof NoSuchElementException){

      }
      else{
        e.printStackTrace();
      }
    }
    return sum;
  }
}
