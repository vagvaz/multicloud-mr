package eu.leads.processor.web;

import data.MetaData;
import data.PluginStatus;
import eu.leads.processor.common.plugins.PluginPackage;
import eu.leads.processor.core.Tuple;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.WebSocket;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.PlatformLocator;
import org.vertx.java.platform.PlatformManager;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Created by vagvaz on 1/28/15.
 */
public class DefaultLeadsQueryProcessorClient implements LeadsQueryProcessorClient {
  private HttpClient httpClient = null;
  private WebSocket webSocket = null;
  private Set<String> pendingQueries;
  private Set<String> registeredEvents;

  private PlatformManager platformManager = null;
  private Vertx vertx = null;
  private String clientId = "";
  private String host = "";
  private int port = -1;
  private static Logger logger = LoggerFactory.getLogger(DefaultLeadsQueryProcessorClient.class.toString());


  @Override public void initialiaze(String host, int port) {
    clientId = UUID.randomUUID().toString();
    if (!host.startsWith("http://")) {
      this.host = "http://" + host;
    } else {
      this.host = host;
    }
    if (port <= 0) {
      logger.error("Could not connect to LEADS webservice: Invalid port " + port);
    }
    this.port = port;

    //Initialize necessary environment properties needed by vertx when running in embedded mode.
    System.getProperties().put("vertx.home", System.getenv("HOME") + "/.vertx_mods/");
    System.getProperties().put("vertx.mods", System.getenv("HOME") + "/.vertx_mods/");
    System.getProperties()
        .put("vertx.clusterManagerFactory", "org.vertx.java.spi.cluster.impl.hazelcast.HazelcastClusterManagerFactory");
    //Get platform Manager almost same api as vertx terminal
    platformManager = PlatformLocator.factory.createPlatformManager();
    //set vertx variable
    vertx = platformManager.vertx();

    //    MultiMap wsSocketMaps
    //Initialize REST API client
    //    httpClient = vertx.createHttpClient().setPort(port).setHost(host).connectWebsocket(host + ":"
    //                                                                                         + port
    //                                                                                         + "/app/",



    //    httpClient = platformManager.vertx()
  }

  @Override public String getClientId() {
    return clientId;
  }

  @Override public Future<Tuple> getAsyncObject(String cache, String key, Collection<String> attributes) {
    return null;
  }

  @Override public Future<ActionResult> putAsyncObject(String cache, String key, Tuple object) {
    return null;
  }

  @Override public Future<ActionResult> putAsyncObject(String cache, String key, JsonObject object) {
    return null;
  }

  @Override public Future<QueryStatus> getAsyncQueryStatus(String queryId) {
    return null;
  }

  @Override public Future<QueryStatus> submitAsyncQuery(String username, String query) {
    return null;
  }

  @Override public Future<QueryStatus> submitAsyncWorkflowQuery(String username, String workflowQuery) {
    return null;
  }

  @Override public Future<PluginStatus> deployAsyncPlugin(String username, String pluginName, String cacheName,
      XMLConfiguration config) {
    return null;
  }

  @Override public Future<PluginStatus> getAsyncPluginStatus(String username, String pluginName, String cacheName) {
    return null;
  }

  @Override public Future<ActionResult> undeployAsyncPlugin(String username, String pluginName, String cacheName) {
    return null;
  }

  @Override public Future<ActionResult> submitAsyncPlugin(String pluginId, PluginPackage plugin) {
    return null;
  }

  @Override public Future<ActionResult> uploadAsyncData(String id, String path, MetaData metadata) {
    return null;
  }

  @Override public Future<ActionResult> uploadAsyncData(String id, byte[] data, MetaData metadata) {
    return null;
  }

  @Override public Future<ActionResult> uploadAsyncData(String id, InputStream data, MetaData metadata) {
    return null;
  }

  @Override public Tuple getObject(String cache, String key, Collection<String> attributes) {
    return null;
  }

  @Override public ActionResult putObject(String cache, String key, Tuple object) {
    return null;
  }

  @Override public ActionResult putObject(String cache, String key, JsonObject object) {
    return null;
  }

  @Override public QueryStatus getQueryStatus(String queryId) {
    return null;
  }

  @Override public QueryStatus submitQuery(String username, String query) {
    return null;
  }

  @Override public QueryStatus submitWorkflowQuery(String username, String workflowQuery) {
    return null;
  }

  @Override
  public PluginStatus deployPlugin(String username, String pluginName, String cacheName, XMLConfiguration config) {
    return null;
  }

  @Override public PluginStatus getPluginStatus(String username, String pluginName, String cacheName) {
    return null;
  }

  @Override public ActionResult undeployPlugin(String username, String pluginName, String cacheName) {
    return null;
  }

  @Override public ActionResult submitPlugin(String pluginId, PluginPackage plugin) {
    return null;
  }

  @Override public ActionResult uploadData(String id, String path, MetaData metadata) {
    return null;
  }

  @Override public ActionResult uploadData(String id, byte[] data, MetaData metadata) {
    return null;
  }

  @Override public ActionResult uploadData(String id, InputStream data, MetaData metadata) {
    return null;
  }
}
