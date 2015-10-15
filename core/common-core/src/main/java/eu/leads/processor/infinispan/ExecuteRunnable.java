package eu.leads.processor.infinispan;

import eu.leads.processor.core.EngineUtils;

import java.util.Map;

/**
 * Created by vagvaz on 8/19/15.
 */
public class ExecuteRunnable implements Runnable {
  Object key;
  Object value;
  private LeadsBaseCallable callable;
  private boolean isRunning = false;

  public ExecuteRunnable(LeadsBaseCallable callable) {
    this.callable = callable;
  }

  public ExecuteRunnable() {

  }

  public void setKeyValue(Object key, Object value, LeadsBaseCallable callable) {
    this.key = key;
    this.value = value;
    this.callable = callable;
  }

  @Override public void run() {
    isRunning = true;
    try {
      Map.Entry entry = null;
      while (callable.isContinueRunning() || !callable.isEmpty()) {
        System.err.println(callable.getCallableIndex()+": "+ callable.isContinueRunning() + " " + callable.isEmpty() + " sz " + callable.getSize());
        entry = callable.poll();
        while (entry != null) {
          key = entry.getKey();
          value = entry.getValue();
          callable.executeOn(key, value);
          entry = callable.poll();
        }
        try {
          Thread.sleep(0, 10000);
          Thread.yield();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      callable = null;
      EngineUtils.addRunnable(this);
      isRunning = false;
    }catch (Exception e ){
      e.printStackTrace();
    }
    isRunning = false;
  }

  public <K, V> void setCallable(LeadsBaseCallable<K, V> callable) {
    this.callable = callable;
  }

  public boolean isRunning() {
    return isRunning;
  }

  public void cancel() {
    isRunning = false;
    System.err.println("in cacnel " + callable.getCallableIndex()+": "+ callable.isContinueRunning() + " " + callable.isEmpty() + " sz " + callable.getSize());
    callable.setContinueRunning(false);
    callable.getInput().clear();
  }
}
