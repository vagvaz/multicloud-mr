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
 * Created by vagvaz on 2/18/15.
 */
public class GenericMapperCallable<K, V, kOut, vOut> extends LeadsBaseCallable<K, V> {
  private String mapperJar;
  private String combinerJar;
  private String mapperClassName;
  private String combinerClassName;
  private byte[] mapperConfig;
  private byte[] combinerConfig;
  private String storageType;
  private Properties storageConfiguration;
  private String intermediateCacheName;
  private LeadsCollector<kOut, vOut> collector;
  private String tmpdirPrefix;

  transient private LeadsMapper mapper;
  transient private LeadsCombiner combiner;
  transient LeadsStorage storageLayer;
  transient Logger log = null;

  public GenericMapperCallable() {
    super();
  }

  public GenericMapperCallable(String configString, String output) {
    super(configString, output);
  }


  @Override public void initialize() {
    //Call super initialization
    log = LoggerFactory.getLogger("GenericCallable" + mapperClassName);
    super.initialize();

    //download mapper from storage layer
    //instatiate and initialize with the given configuration
    storageLayer = LeadsStorageFactory.getInitializedStorage(storageType, storageConfiguration);
    String localMapJarPath = tmpdirPrefix + "/mapreduce/" + mapperJar + "_" + mapperClassName;
    storageLayer.download(mapperJar, localMapJarPath);
    mapper = initializeMapper(localMapJarPath, mapperClassName, mapperConfig);

    //download combiner from storage layer
    //instatiate and initialize with the given configuration
    String localCombinerPath = tmpdirPrefix + "/mapreduce/" + combinerJar + "_" + combinerClassName;
    storageLayer.download(combinerJar, localCombinerPath);
    combiner = initiliazeCombiner(localCombinerPath, combinerClassName, combinerConfig);

    //initialize cllector
    collector.initializeCache(inputCache.getName(), imanager);
    //    collector.setCombiner(combiner);

  }


  private LeadsCombiner initiliazeCombiner(String localCombinerPath, String combinerClassName, byte[] combinerConfig) {
    //Get UrlClassLoader
    //Get instance of the Class
    //Store config to tmpidir
    //initialize combiner instance with config
    //return combiner;
    return null;
  }

  private LeadsMapper initializeMapper(String localMapJarPath, String mapperClassName, byte[] mapperConfig) {
    //Get UrlClassLoader
    //Get instance of the Class
    //Store config to tmpidir
    //initialize combiner instance with config
    //return combiner;


    LeadsMapper result = null;
    ClassLoader classLoader = null;
    try {
      classLoader = ConfigurationUtilities.getClassLoaderFor(localMapJarPath);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }


    //    ConfigurationUtilities.addToClassPath(jarFileName);
    //      .addToClassPath(System.getProperty("java.io.tmpdir") + "/leads/plugins/" + plugName
    //                        + ".jar");

    //    byte[] config = (byte[]) cache.get(plugName + ":conf");
    byte[] config = mapperConfig;
    FSUtilities.flushToTmpDisk(tmpdirPrefix + "/mapreduce/" + mapperJar + "_" + mapperClassName + "-conf.xml", config);
    XMLConfiguration pluginConfig = null;
    try {
      pluginConfig =
          new XMLConfiguration(tmpdirPrefix + "/mapreduce/" + mapperJar + "_" + mapperClassName + "-conf.xml");
    } catch (ConfigurationException e) {
      e.printStackTrace();
    }
    //    String className = (String) cache.get(plugName + ":className");
    String className = mapperClassName;
    if (className != null && !className.equals("")) {
      try {
        Class<?> mapperClass = Class.forName(mapperClassName, true, classLoader);
        Constructor<?> con = mapperClass.getConstructor();
        mapper = (LeadsMapper) con.newInstance();
        //        mapper.initialize(pluginConfig, imanager);
        mapper.initialize(pluginConfig);
        result = mapper;
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
      log.error("Could not find the name for " + mapperClassName);
    }
    return result;
  }

  @Override public void executeOn(K key, V value) {
    mapper.map(key, value, collector);
  }
}
