package eu.leads.processor.web;

import data.MetaData;
import data.PluginStatus;
import eu.leads.processor.common.plugins.PluginPackage;
import eu.leads.processor.core.Tuple;
import org.apache.commons.configuration.XMLConfiguration;
import org.vertx.java.core.json.JsonObject;

import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.Future;

/**
 * Created by vagvaz on 1/18/15.
 * The LeadsQueryprocessor Client interface is the interface implemented by
 * classes that communicate with Leads Query Processor Engine and more specifically its webservice
 * for interacting with the system and developing sophisticated applications.
 * The interface offers an asynchronous and blocking methods.
 */
public interface LeadsQueryProcessorClient {
  public void initialiaze(String host, int port);

  public String getClientId();

  public Future<Tuple> getAsyncObject(String cache, String key, Collection<String> attributes);

  public Future<ActionResult> putAsyncObject(String cache, String key, Tuple object);

  public Future<ActionResult> putAsyncObject(String cache, String key, JsonObject object);

  public Future<QueryStatus> getAsyncQueryStatus(String queryId);

  public Future<QueryStatus> submitAsyncQuery(String username, String query);

  public Future<QueryStatus> submitAsyncWorkflowQuery(String username, String workflowQuery);

  public Future<PluginStatus> deployAsyncPlugin(String username, String pluginName, String cacheName,
      XMLConfiguration config);

  public Future<PluginStatus> getAsyncPluginStatus(String username, String pluginName, String cacheName);

  public Future<ActionResult> undeployAsyncPlugin(String username, String pluginName, String cacheName);

  public Future<ActionResult> submitAsyncPlugin(String pluginId, PluginPackage plugin);

  public Future<ActionResult> uploadAsyncData(String id, String path, MetaData metadata);

  public Future<ActionResult> uploadAsyncData(String id, byte[] data, MetaData metadata);

  public Future<ActionResult> uploadAsyncData(String id, InputStream data, MetaData metadata);

  public Tuple getObject(String cache, String key, Collection<String> attributes);

  public ActionResult putObject(String cache, String key, Tuple object);

  public ActionResult putObject(String cache, String key, JsonObject object);

  public QueryStatus getQueryStatus(String queryId);

  public QueryStatus submitQuery(String username, String query);

  public QueryStatus submitWorkflowQuery(String username, String workflowQuery);

  public PluginStatus deployPlugin(String username, String pluginName, String cacheName, XMLConfiguration config);

  public PluginStatus getPluginStatus(String username, String pluginName, String cacheName);

  public ActionResult undeployPlugin(String username, String pluginName, String cacheName);

  public ActionResult submitPlugin(String pluginId, PluginPackage plugin);

  public ActionResult uploadData(String id, String path, MetaData metadata);

  public ActionResult uploadData(String id, byte[] data, MetaData metadata);

  public ActionResult uploadData(String id, InputStream data, MetaData metadata);
}
