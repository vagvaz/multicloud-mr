package eu.leads.processor.common.infinispan;

import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.plugins.NutchLocalListener;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;

/**
 * Created by vagvaz on 5/23/14.
 */


/**
 * Implementation of InfinispanManager interface This class uses the Distributed Execution of
 * infinispan in order to perform the operations for caches and listeners.
 */
public class ClusterInfinispanManager implements InfinispanManager {


  private Logger log = LoggerFactory.getLogger(this.getClass());
  private EmbeddedCacheManager manager;
  private String configurationFile;
  private HotRodServer server;
  private int serverPort;
  private String host;
  private Configuration defaultConfig = null;
  private Configuration defaultIndexConfig = null;
  private ConfigurationBuilderHolder holder = null;
  private static String uniquePath;
  private String currentComponent;
  private String externalIP = null;
  private int maxEntries;
  //  private static final EquivalentConcurrentHashMapV8<String, TestResources> testResources = new EquivalentConcurrentHashMapV8<>(AnyEquivalence.getInstance(), AnyEquivalence.getInstance());

  /**
   * Constructs a new ClusterInfinispanManager.
   */
  public ClusterInfinispanManager() {
    host = "0.0.0.0";
    serverPort = 11222;
  }

  public ClusterInfinispanManager(EmbeddedCacheManager manager) {
    this.manager = manager;
    initDefaultCacheConfig();
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void setConfigurationFile(String configurationFile) {
    this.configurationFile = configurationFile;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void startManager(String configurationFile) {

    currentComponent =
        LQPConfiguration.getInstance().getConfiguration().getString("node.current.component");
    maxEntries =
        LQPConfiguration.getInstance().getConfiguration()
            .getInt("node.infinispan.maxentries", 3000);
    externalIP =
        LQPConfiguration.getInstance().getConfiguration().getString(StringConstants.PUBLIC_IP);
    if (currentComponent == null) {
      currentComponent = "testingComponents-" + UUID.randomUUID();
    }
    uniquePath = resolveUniquePath();
    ParserRegistry registry = new ParserRegistry();
    //    ConfigurationBuilderHolder holder = null;

    try {
      if (configurationFile != null && !configurationFile.equals("")) {
        holder = registry.parseFile(configurationFile);

      } else {
        System.err.println("\n\n\nUSING DEFAULT FILE ERROR\n\n");
        holder = registry.parseFile(StringConstants.ISPN_CLUSTER_FILE);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    //    GlobalConfigurationBuilder gbuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
    //    Transport transport = gbuilder.transport().getTransport();
    //    gbuilder.transport().transport(transport);
    //    gbuilder.transport().clusterName("cluster");
    //    TransportFlags transportFlags = new TransportFlags();
    //    transportFlags.withReplay2(false);

    TestCacheManagerFactory.amendMarshaller(holder.getGlobalConfigurationBuilder());
    holder.getDefaultConfigurationBuilder().transaction()
        .transactionManagerLookup(new JBossStandaloneJTAManagerLookup());
    GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();
    manager = new DefaultCacheManager(gc, initDefaultCacheConfigBuilder().build(gc), true);
    manager.defineConfiguration("defaultCache", getCacheDefaultConfiguration("defaultCache"));
    manager.getCache("defaultCache");
    manager.getCache();

    //    manager = TestCacheManagerFactory.createClusteredCacheManager(holder.getGlobalConfigurationBuilder(),initDefaultCacheConfigBuilder());
    //    TestCacheManagerFactory.amendGlobalConfiguration(holder.getGlobalConfigurationBuilder(),transportFlags);

    //    manager.getCache();
    //    startHotRodServer(manager,host,serverPort);
    if (LQPConfiguration.getConf().getBoolean("processor.start.hotrod")) {
      host = LQPConfiguration.getConf().getString("node.ip");
      startHotRodServer(manager, host, serverPort);
    }
    //Join Infinispan Cluster
    //      manager.start();
    ConfigurationBuilder
        builder =
        HotRodTestingUtil
            .hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
    //    builder.read(holder.getGlobalConfigurationBuilder().serialization().marshaller(marshaller).build());
    builder.indexing()
        .enable()
        .index(Index.LOCAL)
        .addProperty("default.directory_provider", "filesystem")
        .addProperty("hibernate.search.default.indexBase",
                     "/tmp/leadsprocessor-data/" + uniquePath + "/infinispan/webpage/")
        .addProperty("hibernate.search.default.exclusive_index_use", "true")
        .addProperty("hibernate.search.default.indexmanager", "near-real-time")
        .addProperty("hibernate.search.default.indexwriter.ram_buffer_size", "128")
        .addProperty("lucene_version", "LUCENE_CURRENT");
    builder.clustering().hash().numOwners(1);
    builder.jmxStatistics().enable();
    builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
        //            .persistence().passivation(true)
        .persistence().passivation(false).addSingleFileStore().location
        ("/tmp/leadsprocessor-data/" + uniquePath + "/webpage/")
        .fetchPersistentState(true)
        .shared(false).purgeOnStartup(false).preload(false).expiration().lifespan(-1).maxIdle(-1)
        .wakeUpInterval(-1).reaperEnabled(
        false);
    //    builder.transaction().transactionManagerLookup(new GenericTransactionManagerLookup()).dataContainer().valueEquivalence(AnyEquivalence.getInstance());
    Configuration configuration = builder.build();
    manager.defineConfiguration("WebPage", configuration);
    //    Cache nutchCache = manager.getCache("WebPage", true);

    getPersisentCache("clustered");
    getPersisentCache("pagerankCache");
    getPersisentCache("approx_sum_cache");
    getPersisentCache(StringConstants.STATISTICS_CACHE);
    getPersisentCache(StringConstants.OWNERSCACHE);
    getPersisentCache(StringConstants.PLUGIN_ACTIVE_CACHE);
    getPersisentCache(StringConstants.PLUGIN_CACHE);
    getPersisentCache(StringConstants.QUERIESCACHE);

    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".webpages");
    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".testpages");

    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".entities");

    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".content");
    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".page");
    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".urldirectory");
    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".urldirectory_ecom");
    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".page_core");
    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".keywords");
    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".resourcepart");
    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".site");
    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".adidas_keywords");

    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".rankings");
    getPersisentCache(StringConstants.DEFAULT_DATABASE_NAME + ".uservisits");

    NutchLocalListener
        listener =
        new NutchLocalListener(this, "default.webpages",
                               LQPConfiguration.getInstance().getConfiguration()
                                   .getString("nutch.listener.prefix"), currentComponent);

    manager.getCache("WebPage").addListener(listener);
    //    System.err.println("Loading all the available data from nutch Cache");
    //    final ClusteringDependentLogic cdl = manager.getCache("WebPage").getAdvancedCache().getComponentRegistry()
    //                                           .getComponent
    //                                              (ClusteringDependentLogic.class);
    //    for(Object key : manager.getCache("WebPage").keySet()) {
    //      if (!cdl.localNodeIsPrimaryOwner(key))
    //        continue;
    //      Object value = manager.getCache("WebPage").get(key);
    //      if (value != null) {
    //        listener.processWebPage(key,value);
    //      }
    //    }
    //    getPersisentCache("WebPage");
    //    Marshaller marshaller = null;
    //    try {
    //      marshaller = Util.getInstanceStrict(MARSHALLER, Thread.currentThread().getContextClassLoader());
    //    } catch (ClassNotFoundException e) {
    //      e.printStackTrace();
    //    } catch (InstantiationException e) {
    //      e.printStackTrace();
    //    } catch (IllegalAccessException e) {
    //      e.printStackTrace();
    //    }

    //I might want to sleep here for a little while
    PrintUtilities.printList(manager.getMembers());

    System.out.println("We have started host:" + host);

  }

  private String resolveUniquePath() {

    if (currentComponent.startsWith("testing")) {
      uniquePath = "testing-" + UUID.randomUUID().toString();
    } else {
      //      System.err.println("uniquePath: " + uniquePath + " " + "currentComponent " + currentComponent)
      File file = new File("/tmp/leadsprocessor-data/");
      if (file.exists()) {
        if (file.isDirectory()) {
          File[] files = file.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
              if (name.startsWith(currentComponent)) {
                return true;
              } else {
                return false;
              }
            }
          });
          if (files.length > 0) {
            uniquePath = files[0].toString();
          } else {
            uniquePath = currentComponent + "-" + UUID.randomUUID().toString();
          }
          for (int i = 1; i < files.length; i++) {
            files[i].delete();
          }

        } else {
          log.error("/tmp/leadsprocessor-data is not a directory deleting...");
          file.delete();
          file.mkdirs();
          file.mkdir();
          uniquePath = currentComponent + "-" + UUID.randomUUID().toString();
        }
      } else {
        uniquePath = currentComponent + "-" + UUID.randomUUID().toString();
      }
    }

    return uniquePath;
  }

  private ConfigurationBuilder initDefaultCacheConfigBuilder() {

    ConfigurationBuilder result = null;
    if (LQPConfiguration.getConf()
        .getBoolean("leads.processor.infinispan.persistence", true)) { //perssistence
      if (!LQPConfiguration.getConf().getBoolean("leads.processor.infinispan.useLevelDB", false)) {
        result = new ConfigurationBuilder();
        GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();
        Configuration c = holder.getDefaultConfigurationBuilder().build(gc);
        result.read(c).clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .hash().numOwners(1)
            .indexing().index(Index.NONE).transaction().transactionMode(
            TransactionMode.NON_TRANSACTIONAL)
            .persistence().passivation(true)
            //                                                      .addStore(LevelDBStoreConfigurationBuilder.class)
            //                                                                      .location("/tmp/").shared(true).purgeOnStartup(true).preload(false).compatibility().enable()

            .addSingleFileStore().location("/tmp/leadsprocessor-data/" + uniquePath + "/")
            .fetchPersistentState(true)
            .shared(false).purgeOnStartup(true).preload(false).compatibility().enable()
            .expiration().lifespan(-1).maxIdle(-1).wakeUpInterval(-1).reaperEnabled(
            false).eviction().maxEntries(5000).strategy(EvictionStrategy.LIRS);

      } else { //Use leveldb
        result = new ConfigurationBuilder();
        result.read(holder.getDefaultConfigurationBuilder().build())
            .clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .hash().numOwners(1)
            .indexing().index(Index.NONE).transaction().transactionMode(
            TransactionMode.NON_TRANSACTIONAL)
            .persistence().passivation(true)
            .addStore(LevelDBStoreConfigurationBuilder.class)
            .location("/tmp/leadsprocessor-data/leveldb/" + uniquePath + "-data/")
                //                                 .location("/tmp/leveldb/data-foo/" + "/")
            .expiredLocation("/tmp/leadsprocessor-data/" + uniquePath + "-expired/")
                //                                 .expiredLocation("/tmp/leveldb/expired-foo" + "/")
            .implementationType(LevelDBStoreConfiguration.ImplementationType.JAVA)
            .fetchPersistentState(true)
            .shared(false).purgeOnStartup(true).preload(false).compatibility().enable()
            .expiration().lifespan(-1).maxIdle(-1).wakeUpInterval(-1).reaperEnabled(
            false).eviction().maxEntries(5000).strategy(EvictionStrategy.LIRS)
            .build();
      }
    } else { //do not use persistence
      result = new ConfigurationBuilder();
      result.read(holder.getDefaultConfigurationBuilder().build())
          .clustering()
          .cacheMode(CacheMode.DIST_SYNC)
          .hash().numOwners(1)
          .indexing().index(Index.NONE).transaction().transactionMode(
          TransactionMode.NON_TRANSACTIONAL).compatibility().enable()
          .expiration().lifespan(-1).maxIdle(-1).wakeUpInterval(-1).reaperEnabled(
          false).eviction().maxEntries(5000).strategy(EvictionStrategy.LIRS)
          .build();
    }

    return result;
  }

  public HotRodServer getServer() {
    return server;
  }

  public void setServer(HotRodServer server) {
    this.server = server;
  }

  public int getServerPort() {
    return serverPort;
  }

  public void setServerPort(int serverPort) {
    this.serverPort = serverPort;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  private void startHotRodServer(EmbeddedCacheManager targetManager, String localhost, int port) {
    log.info("Starting HotRod Server");
    System.err.println("Starting HotRod Server");
    serverPort = port;

    boolean isStarted = false;
    while (!isStarted) {
      server = new HotRodServer();
      HotRodServerConfigurationBuilder
          serverConfigurationBuilder =
          new HotRodServerConfigurationBuilder();
      if (externalIP != null && !externalIP.equals("")) {
        serverConfigurationBuilder.host(localhost).port(serverPort).proxyHost(externalIP)
            .proxyPort(11222);
      } else {
        serverConfigurationBuilder.host(localhost).port(serverPort);
      }
      //.defaultCacheName("defaultCache");
      //                 .keyValueFilterFactory("leads-processor-filter-factory",new LeadsProcessorKeyValueFilterFactory(manager))
      //                 .converterFactory("leads-processor-converter-factory",new LeadsProcessorConverterFactory());

      try {
        //       server = TestHelper.startHotRodServer(manager);

        server.start(serverConfigurationBuilder.build(), targetManager);
        server.addCacheEventConverterFactory("leads-processor-converter-factory",
                                             new LeadsProcessorConverterFactory());
        server.addCacheEventFilterFactory("leads-processor-filter-factory",
                                          new LeadsProcessorKeyValueFilterFactory(manager));

        isStarted = true;
      } catch (Exception e) {
        System.out.println("Exception e " + e.getClass().getCanonicalName() + e.getMessage());
        //            if(e == null){
        //               System.out.println(port + " " +)
        //            }
        if (e instanceof NullPointerException) {
          e.printStackTrace();
          server = new HotRodServer();
          serverConfigurationBuilder = new HotRodServerConfigurationBuilder();
          if (externalIP != null && !externalIP.equals("")) {
            serverConfigurationBuilder.host(localhost).port(serverPort).proxyHost(externalIP)
                .proxyPort(serverPort);
          } else {
            serverConfigurationBuilder.host(localhost).port(serverPort);
          }
        } else {
          System.err.println("Not NullPointerExcepiton");
        }
        serverPort++;
        isStarted = false;
        try {
          Thread.sleep(300);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EmbeddedCacheManager getCacheManager() {
    return this.manager;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stopManager() {
    // for(String cacheName : this.manager.getCacheNames())
    //{
    //    Cache cache= this.manager.getCache(cacheName,false);
    //    if(cache != null && cache.getAdvancedCache().getStatus().equals(ComponentStatus.RUNNING))
    //         cache.stop();
    // }
    this.manager.stop();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConcurrentMap getPersisentCache(String name) {
    if (manager.cacheExists(name)) {
      manager.getCache(name);
    }
    //            createCache(name,manager.getDefaultCacheConfiguration());
    else {
      createCache(name, null);
    }
    return manager.getCache(name);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConcurrentMap getPersisentCache(String name, Configuration configuration) {
    if (manager.cacheExists(name)) {
      manager.getCache(name);
    } else {
      createCache(name, configuration);
    }
    return manager.getCache(name);
  }

  @Override
  public ConcurrentMap getIndexedPersistentCache(String name) {
    if (manager.cacheExists(name)) {
      return manager.getCache(name);
    }
    return getIndexedPersistentCache(name, getDefaultIndexedCacheConfiguration());
  }


  @Override
  public ConcurrentMap getIndexedPersistentCache(String name, Configuration configuration) {
    if (manager.cacheExists(name)) {
      return manager.getCache(name);
    } else {
      createIndexedCache(name, configuration);
    }
    return manager.getCache(name);
  }


  private Configuration getDefaultIndexedCacheConfiguration() {
    if (defaultIndexConfig == null) {
      initIndexDefaultCacheConfig();
    }
    return defaultIndexConfig;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removePersistentCache(String name) {
    removeCache(name);
  }

  private void removeCache(String name) {
    try {
      //         if (manager.cacheExists(name)) {
      //            if(manager.getCache(name).getStatus().stopAllowed())
      //               manager.getCache(name).stop();
      //         }
      DistributedExecutorService des = new DefaultExecutorService(manager.getCache());
      List<Future<Void>> list = des.submitEverywhere(new StopCacheCallable(name));
      for (Future<Void> future : list) {
        try {
          future.get(); // wait for task to complete
        } catch (InterruptedException e) {
          log.error(e.getClass().toString() + " while removing " + name + " " + e.getMessage());
        } catch (ExecutionException e) {
          log.error(e.getClass().toString() + " while removing " + name + " " + e.getMessage());
        }
      }
      manager.removeCache(name);
    } catch (Exception e) {
      log.error(
          "Exception while remove " + name + " " + e.getClass().toString() + " " + e.getMessage());
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addListener(Object listener, Cache cache) {
    DistributedExecutorService des = new DefaultExecutorService(cache);
    List<Future<Void>> list = new LinkedList<Future<Void>>();
    //        for (Address a : getMembers()) {

    try {
      list = des.submitEverywhere(new AddListenerCallable(cache.getName(), listener));
      //                list.add(des.submit(a, new AddListenerCallable(cache.getName(), listener)));
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    //        }

    for (Future<Void> future : list) {
      try {
        future.get(); // wait for task to complete
      } catch (InterruptedException e) {
      } catch (ExecutionException e) {
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addListener(Object listener, String name) {
    Cache c = (Cache) this.getPersisentCache(name);
    addListener(listener, c);
  }

  //  /** {@inheritDoc} */
  //  @Override
  //  public void addListener(Object listener, String name, KeyFilter filter) {
  //    Cache c = (Cache) this.getPersisentCache(name);
  //    c.addListener(listener, filter);
  //  }
  //
  //  /** {@inheritDoc} */
  //  @Override
  //  public void addListener(Object listener, String name, KeyValueFilter filter, Converter converter) {
  //    Cache c = (Cache) this.getPersisentCache(name);
  //    c.addListener(listener, filter, converter);
  //
  //  }
  //
  //  /** {@inheritDoc} */
  //  @Override
  //  public void addListener(Object listener, Cache cache, KeyFilter filter) {
  //    cache.addListener(listener, filter);
  //  }
  //
  //  /** {@inheritDoc} */
  //  @Override
  //  public void addListener(Object listener, Cache cache, KeyValueFilter filter, Converter converter) {
  ////        cache.addListener(listener,filter,converter);
  //  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeListener(Object listener, Cache cache) {
    //    DistributedExecutorService des = new DefaultExecutorService(cache);
    //    List<Future<Void>> list = new LinkedList<Future<Void>>();
    //    for (Address a : getMembers()) {
    //      //            des.submitEverywhere(new AddListenerCallable(cache.getName(),listener));
    //      try {
    //        list.add(des.submit(a, new RemoveListenerCallable(cache.getName(), listener)));
    //      } catch (Exception e) {
    //        log.error(e.getMessage());
    //      }
    //    }
    //
    //
    //    for (Future<Void> future : list) {
    //      try {
    //        future.get(); // wait for task to complete
    //      } catch (InterruptedException e) {
    //      } catch (ExecutionException e) {
    //      }
    //    }
    cache.removeListener(listener);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeListener(Object listener, String cacheName) {
    Cache cache = (Cache) getPersisentCache(cacheName);
    removeListener(listener, cache);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Address> getMembers() {
    return manager.getCache("clustered").getAdvancedCache().getRpcManager().getMembers();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Address getMemberName() {
    return manager.getAddress();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isStarted() {
    return manager.getStatus().equals(ComponentStatus.RUNNING);
  }

  private void createCache(String cacheName, Configuration cacheConfiguration) {
//    if (!cacheConfiguration.clustering().cacheMode().isClustered()) {
//      log.warn("Configuration given for " + cacheName
//          + " is not clustered so using default cluster configuration");
//      //            cacheConfiguration = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_ASYNC).async().l1().lifespan(100000L).hash().numOwners(3).build();
//    }
    //      if(manager.cacheExists(cacheName))
    //         return;
    if (cacheName.equals("clustered")) {
      Configuration cacheConfig = getCacheDefaultConfiguration(cacheName);
      manager.defineConfiguration(cacheName, cacheConfig);
      Cache cache = manager.getCache(cacheName);
    } else {
      manager.defineConfiguration(cacheName, getCacheDefaultConfiguration(cacheName));
      DistributedExecutorService des = new DefaultExecutorService(manager.getCache("clustered"));
      List<Future<Void>> list = des.submitEverywhere(new StartCacheCallable(cacheName));
      //
      System.out.println("list " + list.size());
      for (Future<Void> future : list) {
        try {
          future.get(); // wait for task to complete
        } catch (InterruptedException e) {
        } catch (ExecutionException e) {
        }
      }
    }
  }

  private void createIndexedCache(String name, Configuration configuration) {
    manager.defineConfiguration(name, getCacheDefaultConfiguration(name));
    DistributedExecutorService des = new DefaultExecutorService(manager.getCache("clustered"));
    List<Future<Void>> list = des.submitEverywhere(new StartCacheCallable(name, true));
    //
    System.out.println("list " + list.size());
    for (Future<Void> future : list) {
      try {
        future.get(); // wait for task to complete
      } catch (InterruptedException e) {
      } catch (ExecutionException e) {
      }
    }
  }


  private void initDefaultCacheConfig() {
    if (LQPConfiguration.getConf()
        .getBoolean("leads.processor.infinispan.persistence", true)) { //perssistence
      if (!LQPConfiguration.getConf().getBoolean("leads.processor.infinispan.useLevelDB", false)) {
        defaultConfig = new ConfigurationBuilder()//.read(manager.getDefaultCacheConfiguration())
            .clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .hash().numOwners(1)
            .indexing().index(Index.NONE).transaction().transactionMode(
                TransactionMode.NON_TRANSACTIONAL)
            .persistence().passivation(true)
                //                                                      .addStore(LevelDBStoreConfigurationBuilder.class)
                //               .location("/tmp/").shared(true).purgeOnStartup(true).preload(false).compatibility().enable()

            .addSingleFileStore().location("/tmp/leadsprocessor-data/" + uniquePath + "/")
            .fetchPersistentState(true)
            .shared(false).purgeOnStartup(false).preload(false)
            .compatibility().enable()//.marshaller(new TupleMarshaller())
            .expiration().lifespan(-1).maxIdle(-1).wakeUpInterval(-1).reaperEnabled(
                false).eviction().maxEntries(5000).strategy(EvictionStrategy.LIRS)
            .build();

      } else { //Use leveldb
        defaultConfig = new ConfigurationBuilder()//.read(manager.getDefaultCacheConfiguration())
            .clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .hash().numOwners(1)
            .indexing().index(Index.NONE).transaction()
            .transactionMode(TransactionMode.NON_TRANSACTIONAL)
            .persistence().passivation(true)
            .addStore(LevelDBStoreConfigurationBuilder.class)
            .location("/tmp/leadsprocessor-data/leveldb/data-" + uniquePath + "/")
                //                                 .location("/tmp/leveldb/data-foo/" + "/")
            .expiredLocation("/tmp/leadsprocessor-data/expired-" + uniquePath + "/")
                //                                 .expiredLocation("/tmp/leveldb/expired-foo" + "/")
            .implementationType(LevelDBStoreConfiguration.ImplementationType.JAVA)
            .fetchPersistentState(true)
            .shared(false).purgeOnStartup(false).preload(false)
            .compatibility().enable()//.marshaller(new TupleMarshaller())
            .expiration().lifespan(-1).maxIdle(-1).wakeUpInterval(-1).reaperEnabled(false)
            .eviction().maxEntries(5000).strategy(EvictionStrategy.LIRS)
            .build();
      }
    } else { //do not use persistence
      defaultConfig = new ConfigurationBuilder()//.read(manager.getDefaultCacheConfiguration())
          .clustering()
          .cacheMode(CacheMode.DIST_SYNC)
          .hash().numOwners(1)
          .indexing().index(Index.NONE).transaction()
          .transactionMode(TransactionMode.NON_TRANSACTIONAL)
          .compatibility().enable()//.marshaller(new TupleMarshaller())
          .expiration().lifespan(-1).maxIdle(-1).wakeUpInterval(-1).reaperEnabled(false).eviction()
          .maxEntries(5000).strategy(EvictionStrategy.LIRS)
          .build();
    }
  }

  private void initIndexDefaultCacheConfig() {
    if (defaultConfig == null) {
      initDefaultCacheConfig();
    }
    defaultIndexConfig =
        new ConfigurationBuilder().read(defaultConfig).transaction()
            .transactionMode(TransactionMode.NON_TRANSACTIONAL).clustering()
            .cacheMode(CacheMode.REPL_SYNC).l1().disable().indexing().index(Index.ALL)
            .compatibility().enable()
            .build();
  }


  public Configuration getCacheDefaultConfiguration(String cacheName) {
    Configuration cacheConfig = null;
    if (cacheName.equals("clustered") && cacheName.equals("default")) {

      cacheConfig = new ConfigurationBuilder()//.read(manager.getDefaultCacheConfiguration())
          .clustering()
          .cacheMode(CacheMode.DIST_SYNC)
          .hash().numOwners(1)
          .indexing().index(Index.LOCAL).transaction().transactionMode(TransactionMode
                                                                           .NON_TRANSACTIONAL)
          .persistence()
              //                            .addStore(LevelDBStoreConfigurationBuilder.class
              //                            .location("/tmp/").shared(true).purgeOnStartup(true).preload(false).compatibility().enable()
          .addSingleFileStore().location("/tmp/leadsprocessor-data/" + uniquePath + "/")
          .fetchPersistentState(true).purgeOnStartup(false).shared(false).preload(false)
          .compatibility().enable()//.marshaller(new TupleMarshaller())
          .expiration().lifespan(-1).maxIdle(-1).wakeUpInterval(-1).reaperEnabled(false)
          .build();
    } else {
      if (defaultConfig == null) {
        initDefaultCacheConfig();
      }
      cacheConfig = defaultConfig;

    }
    return cacheConfig;
  }

  public Configuration getIndexedCacheDefaultConfiguration(String cacheName) {
    Configuration cacheConfig = null;
    if (cacheName.equals("clustered") && cacheName.equals("default")) {

      cacheConfig = new ConfigurationBuilder()//.read(manager.getDefaultCacheConfiguration())
          .clustering()
          .cacheMode(CacheMode.DIST_SYNC)
          .hash().numOwners(1)
          .indexing().setProperty("auto-config", "true")
          .setProperty("default.directory_provider", "ram").index(Index.ALL).transaction()
          .transactionMode(TransactionMode
                               .NON_TRANSACTIONAL)
          .persistence()
              //                            .addStore(LevelDBStoreConfigurationBuilder.class
              //                            .location("/tmp/").shared(true).purgeOnStartup(true).preload(false).compatibility().enable()
          .addSingleFileStore().location("/tmp/leadsprocessor-data/" + uniquePath + "/")
          .shared(false).preload(false).compatibility().enable()
          .expiration().lifespan(-1).maxIdle(-1).wakeUpInterval(-1).reaperEnabled(false)
          .build();
    } else {
      if (defaultIndexConfig == null) {
        initIndexDefaultCacheConfig();
      }
      cacheConfig = defaultIndexConfig;

    }
    return cacheConfig;
  }


}
