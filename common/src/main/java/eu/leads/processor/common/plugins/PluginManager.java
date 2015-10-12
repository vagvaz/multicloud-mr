package eu.leads.processor.common.plugins;

import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.common.infinispan.PluginHandlerListener;
import eu.leads.processor.common.utils.FSUtilities;
import eu.leads.processor.common.utils.FileLockWrapper;
import eu.leads.processor.common.utils.storage.LeadsStorage;
import eu.leads.processor.common.utils.storage.LeadsStorageFactory;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.plugins.EventType;
import eu.leads.processor.plugins.PluginInterface;
import eu.leads.processor.plugins.SimplePluginRunner;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.io.MD5Hash;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.ensemble.EnsembleCacheManager;
import org.infinispan.ensemble.cache.EnsembleCache;
import org.infinispan.remoting.transport.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by vagvaz on 6/5/14.
 */
public class PluginManager {
  private static final String pluginPrefix = System.getProperty("java.io.tmpdir") + "/" + StringConstants
          .TMPPREFIX + "/pluginstmp/";
  private static Logger log = LoggerFactory.getLogger(PluginManager.class);
  private static LeadsStorage storageLayer;


  public static boolean initialize(String storageType, Properties configuration) {
    try {
      storageLayer = LeadsStorageFactory.getInitializedStorage(storageType, configuration);
      File file = new File(pluginPrefix);
      file.mkdirs();
    } catch (Exception e) {
      log.error(e.getMessage());
      return false;

    }
    return true;
  }

  //WARNING Plugin are uploaded under "plugins" folder!!!!
  public static boolean uploadPlugin(PluginPackage plugin) {
    //        if(!plugin.getId().equals("eu.leads.processor.plugins.pagerank.PagerankPlugin") &&  !plugin.getId().equals("eu.leads.processor.plugins.sentiment")) {
    //Upload jar
    try {
      FileInputStream fileInputStream = new FileInputStream(plugin.getJarFilename());
      byte[] chunk = new byte[1024 * 1024 * 10];
      int chunkCounter = 0;
      storageLayer.delete("plugins/" + plugin.getId() + "/");
      while (fileInputStream.available() > 0) {
        int bytes_red = fileInputStream.read(chunk);
        byte[] uploadBytes = chunk;
        if (bytes_red != chunk.length) {
          uploadBytes = Arrays.copyOf(chunk, bytes_red);
        }

        if (!storageLayer.writeData("plugins/" + plugin.getId() + "/" + chunkCounter, uploadBytes)) {
          log.error("Could Not upload chunk data " + chunkCounter + " failed upload");
          return false;
        } else {
          log.info("Uploaded chunk " + chunkCounter + " size: " + bytes_red);
        }
        chunkCounter++;
      }
    } catch (FileNotFoundException e) {
      log.error(e.getMessage());
      return false;
    } catch (IOException e) {
      log.error(e.getMessage());
      return false;
    }
    plugin.setJarFilename("plugins/" + plugin.getId());
    if (!validatePlugin(plugin)) {
      log.error("Could validate Plugin " + plugin.getId() + " with class name " + plugin.getClassName());
      return false;
    }

    upload(StringConstants.PLUGIN_CACHE, plugin);
    return true;
    //        }
    //        else{
    //            PluginPackage systemPlugin = new PluginPackage(plugin.getId(),plugin.getId());
    //            systemPlugin.setJar(new byte[1]);
    //            systemPlugin.setConfig(plugin.getConfig());
    //            systemPlugin.setId(plugin.getId());
    //            upload(StringConstants.PLUGIN_CACHE, systemPlugin);
    //        }
    //        return true;
  }

  private static boolean validatePlugin(PluginPackage plugin) {
    //        if(plugin.getId().equals("eu.leads.processor.plugins.pagerank.PagerankPlugin")  ||  plugin.getId().equals("eu.leads.processor.plugins.sentiment.SentimentAnalysisPlugin"))
    //            return true;
    String pluginFn = plugin.getJarFilename();
    long plugin_size = storageLayer.size(pluginFn + "/");
    boolean plugin_exists = storageLayer.exists(pluginFn);
    if (!plugin_exists || plugin_size == 0) {
      //        if (plugin.getJar().length == 0) {
      log.error("Tried to upload plugin " + plugin.getId() + " without a valid jar file " + plugin.getJarFilename());
      return false;
    }

    //Donwload plugin to local file system in order to check initialization
    String pluginTmpJar=check_exist_download(pluginFn,plugin,false);
    if(pluginTmpJar==null){
      System.out.println("Wrong file ");
      log.info("Plugin downloaded corrupted?Or not initialized plugin package, MD5 check error");
    }else{
      System.out.println("Correct file ");
    }


    if (!checkPluginNameFromFile(plugin.getClassName(), pluginTmpJar)) {
      log.error("Plugin " + plugin.getId() + " failed validation test. Class.getClassByName("
              + plugin.getClassName() + ")");
      return false;
    }
    if (!checkInstantiate(plugin.getClassName(), pluginTmpJar, plugin.getConfig())) {
      log.error("Plugin " + plugin.getClassName() + " could not create an instance");
      return false;
    }

    return true;
  }
  public static String check_exist_download(String pluginFn, PluginPackage plugin,
      boolean forcedownload) {
    String pluginTmpJar=null;
    System.out.println("Plugin md5: " + plugin.getKey() );
    if(forcedownload)
      pluginTmpJar = downloadPlugin(pluginFn);
    else
      pluginTmpJar = PluginManager.pluginPrefix + pluginFn + ".jar";
    File f = new File(pluginTmpJar);

    if (f.exists() && !f.isDirectory()) {
      //check md5sum
      if (checkMD5(pluginTmpJar, plugin)) {
        return pluginTmpJar;
      } else {
        pluginTmpJar = downloadPlugin(pluginFn);
        if (checkMD5(pluginTmpJar, plugin))
          return pluginTmpJar;
        else
          return null;
      }
    } else {
        pluginTmpJar = downloadPlugin(pluginFn);
        if (checkMD5(pluginTmpJar, plugin))
          return pluginTmpJar;
        else
          return null;
    }
  }

    public static boolean checkMD5(String jarFilname, PluginPackage plugin){
    FileInputStream fileInputStream = null;
      try {
        fileInputStream = new FileInputStream(jarFilname);
        MD5Hash key = MD5Hash.digest(fileInputStream);
        fileInputStream.close();
        System.out.println("checkMD5  key : " + key);
        if (!plugin.check_MD5(key))
          return false;
        else
          return true;

      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    return true;
//    return false;
  }

  private static String downloadPlugin(String jarFilename) {
    String destination_filename = PluginManager.pluginPrefix + jarFilename + ".jar";
    storageLayer.download(jarFilename, destination_filename);
    return destination_filename;
  }

  private static void upload(String pluginCache, PluginPackage plugin) {

    //Add plugin to cache
    Cache cache = (Cache) InfinispanClusterSingleton.getInstance().getManager()
            .getPersisentCache(pluginCache);
    //    if(!plugin.getId().equals("eu.leads.processor.plugins.pagerank.PagerankPlugin") &&  ! plugin.getId().equals("eu.leads.processor.plugins.sentiment.SentimentAnalysisPlugin")) {
    cache.put(plugin.getId(), plugin);


    //    }
    //    else
    //    {
    //      plugin.setJar( new byte[1]);
    //      cache.put(plugin.getId(),plugin);
    //    }
  }

  private static boolean checkPluginName(String className, byte[] jar) {
    boolean result = true;
    String name = System.getProperty("java.io.tmpdir") + "/leads/processor/tmp/testplugin.jar";
    //FileLock
    FileLockWrapper lock = new FileLockWrapper(name);
    lock.lock();

    FSUtilities.flushToDisk(name, jar);
    File file = new File(name);
    ClassLoader cl = null;
    try {
      cl = new URLClassLoader(new URL[]{file.toURI().toURL()});

      Class<?> plugClass = null;

      plugClass = Class.forName(className, true, cl);
    } catch (ClassNotFoundException e) {
      result = false;
    } catch (MalformedURLException e) {
      result = false;
    } finally {
      lock.release();
    }
    return result;
  }

  private static boolean checkPluginNameFromFile(String className, String fileName) {
    boolean result = true;

    File file = new File(fileName);
    ClassLoader cl = null;
    try {
      cl = new URLClassLoader(new URL[]{file.toURI().toURL()},PluginManager.class.getClassLoader());

      Class<?> plugClass = null;

      plugClass = Class.forName(className, true, cl);
    } catch (ClassNotFoundException e) {
      log.error(e.getMessage());
      result = false;
    } catch (MalformedURLException e) {
      result = false;
      log.error(e.getMessage());
    }
    return result;
  }

  private static boolean checkInstantiate(String className, String jarFileName, byte[] config) {
    boolean result = true;
    if(className.toLowerCase().contains("adidas")){
      return true;
    }
    String name = jarFileName;
    String configName = PluginManager.pluginPrefix + "/" + className + "-conf.xml";
    FileLockWrapper jarLock = new FileLockWrapper(name);
    FileLockWrapper confLock = new FileLockWrapper(configName);
    jarLock.lock();
    confLock.lock();

    FSUtilities.flushToDisk(configName, config);
    File file = new File(name);
    ClassLoader cl = null;
    try {
      cl = new URLClassLoader(new URL[]{file.toURI().toURL()},PluginManager.class.getClassLoader());


      Class<?> plugClass = null;

      plugClass = Class.forName(className, true, cl);
      Constructor<?> con = plugClass.getConstructor();
      PluginInterface plug = (PluginInterface) con.newInstance();
      XMLConfiguration pluginConfig = new XMLConfiguration();
      if (config.length > 0) {
        pluginConfig.load(configName);
      }
      InfinispanManager imanager = InfinispanClusterSingleton.getInstance().getManager();
      plug.initialize(pluginConfig, imanager);
      plug.cleanup();
    } catch (MalformedURLException e) {
      result = false;
      log.error(e.getMessage());
    } catch (Exception e) {
      result = false;
      log.error(e.getMessage());
    } finally {
      confLock.release();
      jarLock.release();
    }
    return result;
  }

  private static boolean checkInstantiateFromFile(String className, String jarFileName, String
          configurationString) {
    boolean result = true;
    //    String name = System.getProperty("java.io.tmpdir") + "/leads/processor/tmp/testplugin.jar";
    String configName =
            System.getProperty("java.io.tmpdir") + StringConstants.TMPPREFIX + "/" + className + "-conf.xml";
    //    FileLockWrapper jarLock = new FileLockWrapper(name);
    //    FileLockWrapper confLock = new FileLockWrapper(configName);
    //    jarLock.lock();
    //    confLock.lock();
    //    FSUtilities.flushToDisk(name, jar);
    FSUtilities.flushToDisk(configName, configurationString.getBytes());
    File file = new File(jarFileName);
    ClassLoader cl = null;
    try {
      cl = new URLClassLoader(new URL[]{file.toURI().toURL()});


      Class<?> plugClass = null;

      plugClass = Class.forName(className, true, cl);
      Constructor<?> con = plugClass.getConstructor();
      PluginInterface plug = (PluginInterface) con.newInstance();
      XMLConfiguration pluginConfig = new XMLConfiguration();
      if (configurationString.length() > 0) {
        pluginConfig.load(configName);
      }

      plug.initialize(pluginConfig, InfinispanClusterSingleton.getInstance().getManager());
      plug.cleanup();
    } catch (MalformedURLException e) {
      result = false;
      log.error(e.getMessage());
    } catch (Exception e) {
      result = false;
      log.error(e.getMessage());
    }
    return result;
  }

  public static boolean deployPlugin(String pluginId, String cacheName, EventType[] events, String user) {
    Cache configCache = (Cache) InfinispanClusterSingleton.getInstance().getManager()
            .getPersisentCache(StringConstants.PLUGIN_ACTIVE_CACHE);
    Cache pluginsCache = (Cache) InfinispanClusterSingleton.getInstance().getManager()
            .getPersisentCache(StringConstants.PLUGIN_CACHE);
    boolean result = true;
    PluginPackage plugin = (PluginPackage) pluginsCache.get(pluginId);
    plugin.setUser(user);
    if (plugin == null) {
      result = false;
      log.warn("Plugin " + pluginId + " was not found in plugin cache");
      return false;
    }
    if (!validatePlugin(plugin)) {
      log.warn("Plugin " + plugin.getClassName()
              + " could not be validated so it could not be deployed");
      result = false;
    }
    int eventmask = 0;
    if (events == null || events.length == 0) {
      eventmask = 7;
    } else {
      for (EventType e : events) {
        eventmask += e.getValue();
      }
    }
    addPluginToCache(plugin, eventmask, configCache, cacheName);
    deployPluginListener(plugin.getId(), cacheName, user,
            InfinispanClusterSingleton.getInstance().getManager());

    return result;
  }

  public static void addPluginToCache(PluginPackage plugin, int eventmask, Cache configCache, String
          prefix) {
    configCache.put(prefix + ":" + plugin.getId() + plugin.getUser(), plugin);
    //    configCache.put(prefix+":"+plugin.getId() +user+ ":conf", plugin.getConfig());
    //    configCache.put(prefix+":"+plugin.getId() +user+ ":events", eventmask);
    //    configCache.put(prefix+":"+plugin.getId() +user+ ":className", plugin.getClassName());
    //    configCache.put(prefix+":"+plugin.getId() +user+ ":jar", plugin.getJarFilename());
    //        configCache.getAdvancedCache().withFlags(Flag.FORCE_ASYNCHRONOUS,Flag.SKIP_LISTENER_NOTIFICATION,Flag.SKIP_LOCKING,Flag.SKIP_STATISTICS,Flag.IGNORE_RETURN_VALUES);
    //    if(!plugin.getId().equals("eu.leads.processor.plugins.pagerank.PagerankPlugin") &&  ! plugin.getId().equals("eu.leads.processor.plugins.sentiment.SentimentAnalysisPlugin"))
    //      configCache.put(plugin.getId() + ":jar", plugin.getJar());
    //
    //    configCache.put(plugin.getId() + ":conf", plugin.getConfig());
    //
    //    configCache.put(plugin.getId() + ":events", eventmask);
    //    configCache.put(plugin.getId() + ":className", plugin.getClassName());
  }


  public static PluginHandlerListener deployPluginListener(String pluginId, String cacheName, String user,
                                                           InfinispanManager manager, LeadsStorage storage) {
    Properties conf = new Properties();
    conf.put("target", cacheName);
    conf.put("config", StringConstants.PLUGIN_ACTIVE_CACHE);
    conf.put("user", user);
    ArrayList<String> alist = new ArrayList<String>();
    alist.add(pluginId);
    conf.put("pluginName", alist);
    JsonObject configuration = new JsonObject();
    configuration.putString("targetCache", cacheName);
    configuration.putString("activePluginCache", StringConstants.PLUGIN_ACTIVE_CACHE);
    configuration.putString("pluginName", pluginId);
    configuration.putArray("types", new JsonArray());
    configuration.putString("id", UUID.randomUUID().toString());
    configuration.putString("user", user);
    configuration.getArray("types").add(EventType.CREATED.toString());
    configuration.putString("storageType", storage.getStorageType());
    ByteArrayOutputStream storageConfigurationStream = new ByteArrayOutputStream();
    try {
      storage.getConfiguration().store(storageConfigurationStream, "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    configuration.putBinary("storageConfiguration", storageConfigurationStream.toByteArray());
    //        configuration.getArray("types").add(EventType.MODIFIED);
    //    SimplePluginRunner listener = new SimplePluginRunner("TestSimplePluginDeployer", conf);
    PluginHandlerListener runner = new PluginHandlerListener();

    //    manager.addListener(listener, cacheName);
    RemoteCacheManager remoteCacheManager = createRemoteCacheManager();
    RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache(cacheName);
    //
    System.out.println("Using cache " + cacheName);
    //
    if (remoteCache == null) {
      System.err.println("Cache " + cacheName + " not found!");
      System.exit(1);
    }
    //        if (remoteCache == null) {
    //            System.err.println("Cache " + cacheName + " not found!");
    //            return;
    //        }
    //
    remoteCache.addClientListener(runner, new Object[]{configuration.toString()}, null);
    return runner;
  }

  public static PluginHandlerListener deployPluginListenerWithEvents(String pluginId, String cacheName, String
          user, EventType[] events, InfinispanManager manager, LeadsStorage storage) {
    Properties conf = new Properties();
    conf.put("target", cacheName);
    conf.put("config", StringConstants.PLUGIN_ACTIVE_CACHE);
    conf.put("user", user);
    ArrayList<String> alist = new ArrayList<String>();
    alist.add(pluginId);
    conf.put("pluginName", alist);
    JsonObject configuration = new JsonObject();
    configuration.putString("targetCache", cacheName);
    configuration.putString("activePluginCache", StringConstants.PLUGIN_ACTIVE_CACHE);
    configuration.putString("pluginName", pluginId);
    configuration.putArray("types", new JsonArray());
    configuration.putString("id", UUID.randomUUID().toString());
    configuration.putString("user", user);
    for (EventType e : events) {
      configuration.getArray("types").add(e.toString());
    }
    configuration.putString("storageType", storage.getStorageType());
    ByteArrayOutputStream storageConfigurationStream = new ByteArrayOutputStream();
    try {
      storage.getConfiguration().store(storageConfigurationStream, "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    configuration.putBinary("storageConfiguration", storageConfigurationStream.toByteArray());
    //        configuration.getArray("types").add(EventType.MODIFIED);
    //    SimplePluginRunner listener = new SimplePluginRunner("TestSimplePluginDeployer", conf);
    PluginHandlerListener runner = new PluginHandlerListener();

    //    manager.addListener(listener, cacheName);
    RemoteCacheManager remoteCacheManager = createRemoteCacheManager();
    RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache(cacheName);

    //
    System.out.println("Using cache " + cacheName);
    //
    if (remoteCache == null) {
      System.err.println("Cache " + cacheName + " not found!");
      System.exit(1);
    }
    //        if (remoteCache == null) {
    //            System.err.println("Cache " + cacheName + " not found!");
    //            return;
    //        }
    //
    remoteCache.addClientListener(runner, new Object[]{configuration.toString()}, new Object[0]);
    return runner;
  }

  public static PluginHandlerListener deployPluginListenerWithEvents(String pluginId, String cacheName, String
          user,
                                                                     EventType[] events,
                                                                     EnsembleCacheManager manager, LeadsStorage storage) {
    Properties conf = new Properties();
    conf.put("target", cacheName);
    conf.put("config", StringConstants.PLUGIN_ACTIVE_CACHE);
    conf.put("user", user);
    ArrayList<String> alist = new ArrayList<String>();
    alist.add(pluginId);
    conf.put("pluginName", alist);
    JsonObject configuration = new JsonObject();
    configuration.putString("targetCache", cacheName);
    configuration.putString("activePluginCache", StringConstants.PLUGIN_ACTIVE_CACHE);
    configuration.putString("pluginName", pluginId);
    configuration.putArray("types", new JsonArray());
    configuration.putString("id", UUID.randomUUID().toString());
    configuration.putString("user", user);
    for (EventType e : events) {
      configuration.getArray("types").add(e.toString());
    }
    configuration.putString("storageType", storage.getStorageType());
    ByteArrayOutputStream storageConfigurationStream = new ByteArrayOutputStream();
    try {
      storage.getConfiguration().store(storageConfigurationStream, "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    configuration.putBinary("storageConfiguration", storageConfigurationStream.toByteArray());
    //        configuration.getArray("types").add(EventType.MODIFIED);
    //    SimplePluginRunner listener = new SimplePluginRunner("TestSimplePluginDeployer", conf);
    PluginHandlerListener runner = new PluginHandlerListener();
    try {
      EnsembleCache remoteTargetCache = manager.getCache(cacheName, new ArrayList<>(manager.sites()),
          EnsembleCacheManager.Consistency.DIST);
      log.info("Using cache " + remoteTargetCache.getName() + " to deploy pluign");
      if (remoteTargetCache == null) {
        System.err.println("Cache " + cacheName + " not found!");
        log.error("Target Cache was not found " + cacheName + " not deploying plugin");
        return null;
      }
      remoteTargetCache.addClientListener(runner, new Object[]{configuration.toString()}, new Object[0]);
      //    manager.addListener(listener, cacheName);
//        RemoteCacheManager remoteCacheManager = createRemoteCacheManager();
//      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache(cacheName);

      //
//      System.out.println("Using cache " + cacheName);
      //
//      if (remoteCache == null) {
//        System.err.println("Cache " + cacheName + " not found!");
//        log.error("Target Cache was not found " + cacheName + " not deploying plugin");
//        return null;
//        System.exit(1);
//      }
      //        if (remoteCache == null) {
      //            System.err.println("Cache " + cacheName + " not found!");
      //            return;
      //        }
      //
//      remoteCache.addClientListener(runner, new Object[] {configuration.toString()}, new Object[0]);
    } catch (Exception e) {
      log.error("Trying to add listener to ensemble cache problem");
      e.printStackTrace();
    }
    return runner;
  }

  private static void deployPluginListener(String pluginId, String cacheName, String user,
                                           InfinispanManager manager) {
//    Properties conf = new Properties();
//    conf.put("target", cacheName);
//    conf.put("config", StringConstants.PLUGIN_ACTIVE_CACHE);
//    conf.put("user",user);
    ArrayList<String> alist = new ArrayList<String>();
    alist.add(pluginId);
//    conf.put("pluginName", alist);
    JsonObject configuration = new JsonObject();
    configuration.putString("targetCache", cacheName);
    configuration.putString("activePluginCache", StringConstants.PLUGIN_ACTIVE_CACHE);
    configuration.putString("pluginName", pluginId);
    configuration.putArray("types", new JsonArray());
    configuration.putString("id", UUID.randomUUID().toString());
    configuration.putString("user", user);
    configuration.getArray("types").add(EventType.CREATED.toString());
    configuration.putString("storageType", storageLayer.getStorageType());
    ByteArrayOutputStream storageConfigurationStream = new ByteArrayOutputStream();
    try {
      storageLayer.getConfiguration().store(storageConfigurationStream, "");
    } catch (Exception e) {
      e.printStackTrace();
    }
    configuration.putBinary("storageConfiguration", storageConfigurationStream.toByteArray());
    //        configuration.getArray("types").add(EventType.MODIFIED);
    //    SimplePluginRunner listener = new SimplePluginRunner("TestSimplePluginDeployer", conf);
    PluginHandlerListener runner = new PluginHandlerListener();

    //    manager.addListener(listener, cacheName);
    RemoteCacheManager remoteCacheManager = createRemoteCacheManager();
    RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache(cacheName);
    //
    System.out.println("Using cache " + cacheName);
    //
    if (remoteCache == null) {
      System.err.println("Cache " + cacheName + " not found!");
      System.exit(1);
    }
    //        if (remoteCache == null) {
    //            System.err.println("Cache " + cacheName + " not found!");
    //            return;
    //        }
    //
    remoteCache.addClientListener(runner, new Object[]{configuration.toString()}, new Object[0]);
  }

  private static RemoteCacheManager createRemoteCacheManager() {
    ConfigurationBuilder builder = new ConfigurationBuilder();
    builder.addServer().host(LQPConfiguration.getConf().getString("node.ip")).port(11222);
    return new RemoteCacheManager(builder.build());
  }


  public static boolean deployPlugin(String pluginId, XMLConfiguration config, String cacheName,
                                     EventType[] events, String user) {
    Cache configCache = (Cache) InfinispanClusterSingleton.getInstance().getManager()
            .getPersisentCache(StringConstants.PLUGIN_ACTIVE_CACHE);
    Cache pluginsCache = (Cache) InfinispanClusterSingleton.getInstance().getManager()
            .getPersisentCache(StringConstants.PLUGIN_CACHE);
    boolean result = true;
    PluginPackage plugin = (PluginPackage) pluginsCache.get(pluginId);
    plugin.setUser(user);
    if (plugin == null) {
      result = false;
      log.warn("Plugin " + pluginId + " was not found in plugin cache");
    }

    //Set configuration for the plugin to deploy
    plugin.setConfig(FSUtilities.getBytesFromConfiguration(config));
    if (!validatePlugin(plugin)) {
      log.warn("Plugin " + plugin.getClassName()
              + " could not be validated so it could not be deployed");
      result = false;
    }

    int eventmask = computeEventMask(events);
    addPluginToCache(plugin, eventmask, configCache, configCache.getName());
    deployPluginListener(plugin.getId(), cacheName, user,
            InfinispanClusterSingleton.getInstance().getManager());

    return true;
  }

  private static int computeEventMask(EventType[] events) {
    int eventmask = 0;
    if (events == null || events.length == 0) {
      eventmask = 7;
    } else {
      for (EventType e : events) {
        eventmask += e.getValue();
      }
    }
    return eventmask;
  }

  public static boolean undeploy(String pluginId, String cacheName) {
    InfinispanManager manager = InfinispanClusterSingleton.getInstance().getManager();
    Cache active = (Cache) manager.getPersisentCache(StringConstants.PLUGIN_ACTIVE_CACHE);
    Cache cache = (Cache) manager.getPersisentCache(cacheName);
    DistributedExecutorService des = new DefaultExecutorService(cache);
    List<Future<Void>> list = new LinkedList<Future<Void>>();
    for (Address a : manager.getMembers()) {
      //            des.submitEverywhere(new AddListenerCallable(cache.getName(),listener));
      try {
        list.add(des.submit(a, new UndeployPluginCallable(cache.getName(), pluginId)));
      } catch (Exception e) {
        log.error(e.getMessage());
        return false;
      }
    }


    for (Future<Void> future : list) {
      try {
        future.get(); // wait for task to complete
      } catch (InterruptedException e) {
        return false;
      } catch (ExecutionException e) {
        return false;
      }
    }
    return true;
  }

  public static void deployLocalPlugin(PluginInterface plugin, XMLConfiguration config,
                                       String cacheName, EventType[] events,
                                       InfinispanManager manager) {
    Properties conf = new Properties();
    conf.put("target", cacheName);
    conf.put("config", StringConstants.PLUGIN_ACTIVE_CACHE);
    LinkedList<String> alist = new LinkedList<String>();
    alist.add(plugin.getClassName());
    conf.put("pluginNames", alist);
    SimplePluginRunner runner = new SimplePluginRunner("test-local", conf);
    runner.initialize(manager);
    int eventmask = computeEventMask(events);
    runner.addPlugin(plugin, config, eventmask);
    manager.addListener(runner, cacheName);

  }

  public static boolean uploadInternalPlugin(PluginPackage plugin) {
    if (!validatePlugin(plugin)) {
      log.error("Could validate Plugin " + plugin.getId() + " with class name " + plugin.getClassName());
      return false;
    }
    upload(StringConstants.PLUGIN_CACHE, plugin);
    return true;

  }
}
