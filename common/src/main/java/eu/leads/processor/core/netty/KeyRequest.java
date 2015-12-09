package eu.leads.processor.core.netty;

import java.io.Serializable;

/**
 * Created by vagvaz on 12/9/15.
 */
public class KeyRequest implements Serializable {
  String key;
  String cache;
  Serializable value;

  public KeyRequest(String cache,String key){
    this.key = key;
    this.cache = cache;
    this.value = null;
  }

  public KeyRequest(String cache,String key,Serializable value){
    this.key = key;
    this.cache = cache;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getCache() {
    return cache;
  }

  public void setCache(String cache) {
    this.cache = cache;
  }

  public Serializable getValue() {
    return value;
  }

  public void setValue(Serializable value) {
    this.value = value;
  }
}
