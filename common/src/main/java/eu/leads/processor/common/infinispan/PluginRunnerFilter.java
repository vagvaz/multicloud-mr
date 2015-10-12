package eu.leads.processor.common.infinispan;

import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.plugins.PluginManager;
import eu.leads.processor.common.plugins.PluginPackage;
import eu.leads.processor.common.utils.FSUtilities;
import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.common.utils.storage.LeadsStorage;
import eu.leads.processor.common.utils.storage.LeadsStorageFactory;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.plugins.EventType;
import eu.leads.processor.plugins.PluginInterface;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.jgroups.util.ConcurrentLinkedBlockingQueue;
import org.jgroups.util.ConcurrentLinkedBlockingQueue2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static eu.leads.processor.plugins.EventType.*;

/**
 * Created by vagvaz on 9/29/14.
 */
public class PluginRunnerFilter implements CacheEventFilter,Serializable {

  transient private JsonObject conf;
  private String configString;
  private String UUIDname;
  transient private EmbeddedCacheManager manager;
  transient private ClusterInfinispanManager imanager;
  transient private Cache pluginsCache;
  transient private  Cache targetCache;
  transient private String targetCacheName;

  transient private Logger log ;//= LoggerFactory.getLogger(PluginRunnerFilter.class) ;
  transient private PluginInterface plugin;
  transient private String pluginsCacheName;
  transient private String pluginName;
  transient private List<EventType> type;
  transient private boolean isInitialized =false ;
  transient private LeadsStorage storageLayer;// = null;
  transient private String user;
  private transient volatile Object mutex = new Object();
  private transient ThreadPoolExecutor executor;
  private transient ProfileEvent profEvent;

  public PluginRunnerFilter(EmbeddedCacheManager manager,String confString){
    log = LoggerFactory.getLogger(manager.getAddress().toString()+":"+PluginRunnerFilter.class.toString());
    log.error("Manager init");
    this.manager = manager;
    log.error("set config string");
    this.configString = confString;
    log.error("initJson conf");
    this.conf = new JsonObject(configString);
    log.error("init Imanager");
    imanager = new ClusterInfinispanManager(InfinispanClusterSingleton.getInstance().getManager().getCacheManager());
    executor = new ThreadPoolExecutor(1,1,10000, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
    log.error("init");
    UUIDname = UUID.randomUUID().toString();

    System.err.println(UUIDname + " Construct " + confString);
    storageLayer=null;
  }

  private void writeObject(ObjectOutputStream o)
          throws IOException {
    o.defaultWriteObject(); //is enough for no transient objects but anyway
    o.writeUTF(UUIDname);
    o.writeUTF(configString);
    System.err.println(UUIDname + " Serialize " + configString);
  }

  private void readObject(ObjectInputStream i)
          throws IOException, ClassNotFoundException {
    i.defaultReadObject();
    UUIDname =  i.readUTF();
    configString =  i.readUTF();
    isInitialized = false;
    UUIDname = UUID.randomUUID().toString();
    mutex = new Object();
    System.err.println(UUIDname+" DeSerialize " + configString);
    initialize();
  }

  private void initialize() {
      try {
        if (isInitialized) {
          System.err.println("Already initialized !!!! not again  " + UUIDname);
          return;
        }
        executor = new ThreadPoolExecutor(1,1,10000, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        System.err.println("Initilize " + UUIDname);
        log = LoggerFactory.getLogger(PluginRunnerFilter.class);
        log = LoggerFactory
            .getLogger(UUIDname + " PluginRunner." + pluginName + ":" + pluginsCacheName);
        profEvent = new ProfileEvent("InitialLog",log);
        this.manager = InfinispanClusterSingleton.getInstance().getManager().getCacheManager();
        this.conf = new JsonObject(configString);
        imanager = (ClusterInfinispanManager) InfinispanClusterSingleton.getInstance().getManager();

        log.error("get activePluginCache");
        pluginsCacheName = conf.getString("activePluginCache");//StringConstants.PLUGIN_ACTIVE_CACHE);

        log.error("get pluginName");
        pluginName = conf.getString("pluginName");
        log.error("get user");
        user = conf.getString("user");
        log.error("get types");
        JsonArray types = conf.getArray("types");
        //InferTypes

        type = new ArrayList<EventType>(3);
        if (types != null) {
          Iterator<Object> iterator = types.iterator();
          if (iterator.hasNext()) {
            log.error("READ EVent type ");
            type.add(EventType.valueOf(iterator.next().toString()));
          }
        }

        if (type.size() == 0) {
          type.add(CREATED);
          type.add(REMOVED);
          type.add(MODIFIED);
        }

        log.error("init pluginscache " + UUIDname);
        pluginsCache = (Cache) imanager.getPersisentCache(pluginsCacheName);
        log.error("init logger");
        //    log = LoggerFactory.getLogger(managerAddress+ " PluginRunner."+pluginName+":"+
        //                                    pluginsCacheName);
        log.error("init storage");
        String storagetype = this.conf.getString("storageType");
        Properties storageConfiguration = new Properties();
        byte[] storageConfBytes = this.conf.getBinary("storageConfiguration");
        ByteArrayInputStream bais = new ByteArrayInputStream(storageConfBytes);
        try {
          storageConfiguration.load(bais);
          storageLayer = LeadsStorageFactory.getInitializedStorage(storagetype, storageConfiguration);
        } catch (IOException e) {
          e.printStackTrace();
        }
        log.error("init targetCacheName");
        targetCacheName = conf.getString("targetCache");
        log.error("init Targetcache");
        targetCache = (Cache) imanager.getPersisentCache(targetCacheName);
        log.error("init plugin");
        Thread t = new Thread(new Runnable() {
          @Override public void run() {
            initializePlugin(pluginsCache, pluginName, user);
            System.err.println("Plugin " + plugin.getClassName().toString() + " Loaded from jar and initialized");
          }
        });
        t.start();
//            initializePlugin(pluginsCache, pluginName, user);
        isInitialized = true;
        log.error("Initialized plugin " + pluginName + " on " + targetCacheName);
        System.err.println("Initialized plugin " + pluginName + " on " + targetCacheName);

      } catch (Exception e) {
        e.printStackTrace();
      }
  }


  private void initializePlugin(Cache cache, String plugName, String user) {

    log.error("INITPLUG:" + targetCacheName + ":" + plugName + user);
    PluginPackage pluginPackage = (PluginPackage) cache.get(targetCacheName+":"+plugName+user);
    log.error("INITPLUG:" + (pluginPackage == null));
    String tmpdir = System.getProperties().getProperty("java.io.tmpdir")+"/"+StringConstants
              .TMPPREFIX+"/runningPlugins/"+ UUIDname+"/";//+ UUID.randomUUID().toString()+"/";


    log.error("using tmpdir " + tmpdir);
    String  jarFileName = tmpdir+pluginPackage.getClassName()+".jar";
    log.error("Download... " + "plugins/" + plugName + " -> " + jarFileName);
    System.out.println("Download... " + "plugins/" + plugName + " -> " + jarFileName);
    check_exist_download(jarFileName,plugName,pluginPackage,false);
    //PluginManager.checkMD5(jarFileName,pluginPackage);
    log.error("Downloaded " + "plugins/" + plugName + " -> " + jarFileName);
    System.out.println("Downloaded " + "plugins/" + plugName + " -> " + jarFileName);
    //storageLayer.download("plugins/" + plugName, jarFileName);
    ClassLoader classLoader = null;


    File file = new File(jarFileName);
    try {
      if(!pluginName.toLowerCase().contains("adidas")) {
        classLoader = new URLClassLoader(new URL[] {file.toURI().toURL()}, InfinispanClusterSingleton.class.getClassLoader());
      }
      else{
        classLoader = new URLClassLoader(new URL[] {file.toURI().toURL()}, InfinispanClusterSingleton.class.getClassLoader());
      }

    } catch (MalformedURLException e) {
      log.error("exception " + e.getClass().toString());
      log.error("exception " + e.getMessage());
      e.printStackTrace();
    }


    //    ConfigurationUtilities.addToClassPath(jarFileName);
//      .addToClassPath(System.getProperty("java.io.tmpdir") + "/leads/plugins/" + plugName
//                        + ".jar");

//    byte[] config = (byte[]) cache.get(plugName + ":conf");

    byte[] config = pluginPackage.getConfig();
    log.error("Flush config to dist " + tmpdir + plugName + "-conf.xml " + config.length);
    FSUtilities.flushToTmpDisk(tmpdir + plugName + "-conf.xml", config);
    log.error("read config");
    XMLConfiguration pluginConfig = null;
    try {
      pluginConfig =
        new XMLConfiguration(tmpdir + plugName + "-conf.xml");
    } catch (ConfigurationException e) {
      log.error("exception " + e.getClass().toString());
      log.error("exception " + e.getMessage());
      e.printStackTrace();
    }
//    String className = (String) cache.get(plugName + ":className");
    String className = pluginPackage.getClassName();
    log.error("Init plugClass");
    if (className != null && !className.equals("")) {
      try {
        Class<?> plugClass =
          Class.forName(className, true, classLoader);
        Constructor<?> con = plugClass.getConstructor();
        log.error("get plugin new Instance");
        plugin = (PluginInterface) con.newInstance();
        log.error("initialize plugin ");
        plugin.initialize(pluginConfig, imanager);

      } catch (ClassNotFoundException e) {
        log.error("exception " + e.getClass().toString());
        log.error("exception " + e.getMessage());
        e.printStackTrace();
      } catch (NoSuchMethodException e) {
        log.error("exception " + e.getClass().toString());
        log.error("exception " + e.getMessage());
        e.printStackTrace();
      } catch (InvocationTargetException e) {
        log.error("exception " + e.getClass().toString());
        log.error("exception " + e.getMessage());
        e.printStackTrace();
      } catch (InstantiationException e) {
        log.error("exception " + e.getClass().toString());
        log.error("exception " + e.getMessage());
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        log.error("exception " + e.getClass().toString());
        log.error("exception " + e.getMessage());
        e.printStackTrace();
      }
      catch (Exception e){
        log.error("exception " + e.getClass().toString());
        log.error("exception " + e.getMessage());
      }
    } else {
      log.error("Could not find the name for " + plugName);
    }
  }
  private String check_exist_download(String jarFileName, String plugName,  PluginPackage plugin, boolean forcedownload) {

    System.out.println("Plugin md5: " + plugin.getKey() );
    if(forcedownload)
      storageLayer.download("plugins/" + plugName, jarFileName);

    File f = new File(jarFileName);

    if (f.exists() && !f.isDirectory()) {
      //check md5sum
      System.out.println("File already exists " );
      if (PluginManager.checkMD5(jarFileName, plugin)) {
        System.out.println("MD5 correct no redownloading " );
        return jarFileName;
      } else {
        System.out.println(" MD5 checksum error redownload plugin, pluginPackage key: " + plugin.getKey());
        storageLayer.download("plugins/" + plugName, jarFileName);
        if (PluginManager.checkMD5(jarFileName, plugin)) {
          System.out.println("MD5 correct  " );
          return jarFileName;
        }else{
          System.err.println("MD5 incorrect  " );
          return null;
        }
      }
    } else {
      System.out.println("File does not exists " );
      storageLayer.download("plugins/" + plugName, jarFileName);
      if (PluginManager.checkMD5(jarFileName, plugin)) {
        System.out.println("File downloaded  " );
        return jarFileName;
      }else {
        System.err.println("MD5 incorrect  " );
        return null;
      }
    }
  }

  @Override
  public boolean accept(Object key, Object oldValue, Metadata oldMetadata, Object newValue,
                         Metadata newMetadata,
                         org.infinispan.notifications.cachelistener.filter.EventType eventType) {
    try {
        if (!isInitialized) {
          initialize();
        }
        if(plugin == null)
        {
          System.out.println("Plugin null");
          return false;
        }

      if(plugin.getClassName() == null)
      {
        System.out.println("Plugin ClassName");
        return false;
      }
      if(key == null){
        System.out.println("key null");
        return false;
      }
        profEvent.end("");
        profEvent.start("Plugin: " + plugin.getClassName().toString() + " key " + key.toString());
        System.err.println(UUIDname + " Accept: Manager Address " + manager.getAddress());
        log.error(UUIDname + "Accept: Manager Address " + manager.getAddress());

        if (key == null) {
          log.error("null key:");
          return false;
        }

        if (newValue == null) {
          System.out.println("newValue is null key:" + (String) key);
          log.error("Accept newValue is null key:" + (String) key);
          return false;
        }
        //    else{
        //      System.out.println("key:" + (String)key+", newValue is " + newValue);
        //      log.info("Accept, key:" + (String)key+", newValue is " + newValue);
        //    }
        String o1 = (String) key;
        //    Object value = newValue;
        Object value = null;
        switch (eventType.getType()) {
          case CACHE_ENTRY_CREATED:
            value = newValue;
            break;
          case CACHE_ENTRY_REMOVED:
            value = oldValue;
            break;
          case CACHE_ENTRY_MODIFIED:
            value = newValue;
            break;
          default:
            break;
        }

        if (value instanceof Tuple) {
          value = value.toString();
        }
      PluginRunnable runnable = new PluginRunnable(this,plugin);
        switch (eventType.getType()) {
          case CACHE_ENTRY_CREATED:
            if (type.contains(CREATED)) {
              //              plugin.created(key, value, targetCache);
              runnable.setParameters(key, value, targetCache, EventType.CREATED);
            }
            break;
          case CACHE_ENTRY_REMOVED:
            if (type.contains(REMOVED)) {
              if (oldValue == null) {
                log.error("Accept old value is null");
                return false;
              }
              //          value = (String) oldValue;
//              plugin.removed(key, value, targetCache);
              runnable.setParameters(key,value,targetCache,EventType.REMOVED);
            }
            break;
          case CACHE_ENTRY_MODIFIED:
            if (type.contains(MODIFIED))
//              plugin.modified(key, value, targetCache);
              runnable.setParameters(key,value,targetCache,EventType.MODIFIED);
            break;
          default:
            break;
        }
      executor.submit(runnable);
      profEvent.end();
        return false;
      }catch(Exception e){
        e.printStackTrace();
      }

      return false;
  }


}
