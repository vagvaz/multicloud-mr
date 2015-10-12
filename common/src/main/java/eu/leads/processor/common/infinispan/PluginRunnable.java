package eu.leads.processor.common.infinispan;

import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.plugins.EventType;
import eu.leads.processor.plugins.PluginInterface;
import org.infinispan.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by vagvaz on 9/18/15.
 */
public class PluginRunnable implements Runnable{

  private PluginRunnerFilter runnerFilter;
  private Object key;
  private Object value;
  private Cache cache;
  private EventType type;
  private PluginInterface plugin;
  private ProfileEvent event;
  private Logger log;
  public PluginRunnable(PluginRunnerFilter filter,PluginInterface pluigin){
    this.runnerFilter = filter;
    this.plugin = pluigin;
    log = LoggerFactory.getLogger(PluginRunnable.class);
    event = new ProfileEvent("Nothing",log);
  }
  @Override public void run() {
    event.start("PLUGINRunnable: plugin-> " + plugin.getId() + " key " + key.toString());
    log.error("Rum plugin " + plugin.getId() + " for " + key.toString());
    try {
      switch (type) {
        case CREATED:
          plugin.created(key, value, cache);
          break;
        case MODIFIED:
          plugin.modified(key, value, cache);
          break;
        case REMOVED:
          plugin.removed(key, value, cache);
          break;
      }
      event.end();
    }catch (Exception e){
      e.printStackTrace();
      PrintUtilities.logStackTrace(log,e.getStackTrace());
      event.end();
    }
    release();
//    runnerFilter.addRunnable(this);

  }

  private void release() {
    key = null;
    value = null;
    cache = null;
    type = null;
  }

  public void setParameters(Object key,Object value,Cache cache,EventType type){
    this.key = key;
    this.value = value;
    this.cache = cache;
    this.type = type;
  }
}
