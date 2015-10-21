package eu.leads.processor.infinispan;

import eu.leads.processor.common.utils.FSUtilities;
import eu.leads.processor.common.utils.storage.LeadsStorage;
import eu.leads.processor.common.utils.storage.LeadsStorageFactory;
import eu.leads.processor.conf.ConfigurationUtilities;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Created by vagvaz on 2/19/15.
 */
public class GenericReducerCallable<K, V> extends LeadsBaseCallable<K, Object> {
  private static final long serialVersionUID = 3724554288677416503L;
  //  private String outputCacheName;
  private String reducerJar;
  private String reducerClassName;
  private byte[] reducerConfig;
  private String storageType;
  private Properties storageConfiguration;
  private String outputCacheName;
  private LeadsCollector<K, V> collector;
  private String tmpdirPrefix;
  private String prefix;
  transient private LeadsReducer reducer;
  transient LeadsStorage storageLayer;
  transient private Logger log = null;

  public GenericReducerCallable() {
    super();
  }

  public GenericReducerCallable(String configString, String output) {
    super(configString, output);
  }

  @Override public void initialize() {
    //Call super initialization
    super.initialize();
    log = LoggerFactory.getLogger(GenericReducerCallable.class);
    //download mapper from storage layer
    //instatiate and initialize with the given configuration
    storageLayer = LeadsStorageFactory.getInitializedStorage(storageType, storageConfiguration);
    String localMapJarPath = tmpdirPrefix + "/mapreduce/" + reducerJar + "_" + reducerClassName;
    storageLayer.download(reducerJar, localMapJarPath);
    reducer = initializeReducer(localMapJarPath, reducerClassName, reducerConfig);
    //initialize cllector
    collector.initializeCache(inputCache.getName(), imanager);
    //    collector.setCombiner(combiner);
  }

  private LeadsReducer initializeReducer(String localReduceJarPath, String reducerClassName, byte[] reducerConfig) {

    LeadsReducer result = null;
    ClassLoader classLoader = null;
    try {
      classLoader = ConfigurationUtilities.getClassLoaderFor(localReduceJarPath);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }


    //    ConfigurationUtilities.addToClassPath(jarFileName);
    //      .addToClassPath(System.getProperty("java.io.tmpdir") + "/leads/plugins/" + plugName
    //                        + ".jar");

    //    byte[] config = (byte[]) cache.get(plugName + ":conf");
    byte[] config = reducerConfig;
    FSUtilities
        .flushToTmpDisk(tmpdirPrefix + "/mapreduce/" + reducerJar + "_" + reducerClassName + "-conf.xml", config);
    XMLConfiguration pluginConfig = null;
    try {
      pluginConfig =
          new XMLConfiguration(tmpdirPrefix + "/mapreduce/" + reducerJar + "_" + reducerClassName + "-conf.xml");
    } catch (ConfigurationException e) {
      e.printStackTrace();
    }
    //    String className = (String) cache.get(plugName + ":className");
    String className = reducerClassName;
    if (className != null && !className.equals("")) {
      try {
        Class<?> mapperClass = Class.forName(reducerClassName, true, classLoader);
        Constructor<?> con = mapperClass.getConstructor();
        reducer = (LeadsReducer) con.newInstance();
        //        mapper.initialize(pluginConfig, imanager);
        reducer.initialize(pluginConfig);
        result = reducer;
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (NoSuchMethodException e) {
        e.printStackTrace();
      } catch (InvocationTargetException e) {
        e.printStackTrace();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    } else {
      log.error("Could not find the name for " + reducerClassName);
    }
    return result;
  }

  @Override public void executeOn(K key, Object value) {
    LeadsIntermediateIterator iterator = new LeadsIntermediateIterator(key.toString(), prefix, imanager);
    reducer.reduce(key, iterator, collector);
  }
}
