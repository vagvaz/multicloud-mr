package eu.leads.processor.nqe;

import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.conf.ConfigurationUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.ActionHandler;
import eu.leads.processor.core.ActionStatus;
import eu.leads.processor.core.comp.LeadsMessageHandler;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.DefaultNode;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.core.netty.IndexManager;
import eu.leads.processor.core.netty.NettyDataTransport;
import eu.leads.processor.core.plan.QueryState;
import eu.leads.processor.core.plan.QueryStatus;
import eu.leads.processor.imanager.IManagerConstants;
import eu.leads.processor.imanager.RemoveListenerActionHandler;
import eu.leads.processor.nqe.handlers.*;
import eu.leads.processor.web.WebServiceClient;
import org.infinispan.Cache;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static eu.leads.processor.core.ActionStatus.INPROCESS;
import static eu.leads.processor.core.ActionStatus.valueOf;

/**
 * Created by vagvaz on 8/6/14.
 */
public class NQEProcessorWorker extends Verticle implements Handler<Message<JsonObject>> {

  Node com;
  String id;
  String gr;
  String workqueue;
  String logic;
  JsonObject config;
  EventBus bus;
  LeadsMessageHandler leadsHandler;
  LogProxy log;
  InfinispanManager persistence;
  Map<String, ActionHandler> handlers;
  Map<String, Action> activeActions;
  String currentCluster;
  JsonObject globalConfig;
  private Cache jobsCache;

  @Override public void start() {
    super.start();
    activeActions = new HashMap<String, Action>();
    leadsHandler = new LeadsMessageHandler() {
      @Override public void handle(JsonObject event) {
        if (event.getString("type").equals("unregister")) {
          JsonObject msg = new JsonObject();
          msg.putString("processor", id + ".process");
          com.sendWithEventBus(workqueue + ".unregister", msg);
          stop();
        } else if (event.getString("type").equals("action")) {
          Action action = new Action(event);
          switch (valueOf(action.getStatus())) {
            case COMPLETED:
              if (action.getLabel().equals(NQEConstants.DEPLOY_OPERATOR)) {
                log.info("Operator: " + action.getData().getString("operatorType") + " is completed");
                com.sendTo(action.getData().getString("monitor"), action.asJsonObject());
                activeActions.remove(action.getId());
              } else if (action.getLabel().equals(NQEConstants.DEPLOY_REMOTE_OPERATOR)) {
                Action replyAction = new Action(action.getData());
                String coordinator = action.asJsonObject().getString("coordinator");
                replyAction.getData().putString("microcloud", currentCluster); //reduncdany to speed
                // up debuggin
                //                       replyAction.getData().putString("microcloud",currentCluster);
                replyAction.getData().putString("STATUS", "SUCCESS");
                replyAction.getData().putString("replyGroup", action.asJsonObject().getString("replyGroup"));

                String webaddress = getURIFromGlobal(coordinator);
                try {
                  WebServiceClient.completeMapReduce(replyAction.asJsonObject(), webaddress);
                } catch (IOException e) {
                  e.printStackTrace();
                }
                //                       System.err.println("Remote DEPLOY of " + action.getData().getObject("operator").getObject
                //                                                                                                         ("configuration").toString() + " was successful");
                log.error("Remote DEPLOY of " + action.getId() + " was successful");
              } else if (action.getLabel().equals(NQEConstants.EXECUTE_MAP_REDUCE_JOB)) {
                String id = action.getData().getObject("operator").getString("id");
                String s = (String) jobsCache.get(id);
                JsonObject o = new JsonObject(s);
                QueryStatus queryStatus = new QueryStatus(o.getObject("status"));
                queryStatus.setStatus(QueryState.COMPLETED);
                o.putObject("status", queryStatus.asJsonObject());
                //                QueryStatus queryStatus = new QueryStatus(new JsonObject(s));
                //                queryStatus.setStatus(QueryState.COMPLETED);
                jobsCache.put(id, o.toString());
              } else {
                log.error("COMPLETED Action " + action.toString() + "Received by NQEProcessor but cannot be handled");
              }
              break;
            case PENDING:
              if (action.getLabel().equals(NQEConstants.OPERATOR_GET_RUNNING_STATUS)) {
                Action runningAction = new Action(action.asJsonObject().copy());
                runningAction.setLabel(NQEConstants.OPERATOR_RUNNING_STATUS);
                com.sendTo(action.getData().getString("replyTo"), runningAction.asJsonObject());
              } else if (action.getLabel().equals(NQEConstants.OPERATOR_GET_OWNER)) {
                Action runningAction = new Action(action.asJsonObject().copy());
                runningAction.setLabel(NQEConstants.OPERATOR_OWNER);
                runningAction.getData().putString("owner", com.getId());
                runningAction.setStatus(INPROCESS.toString());
                com.sendTo(action.getData().getString("replyTo"), runningAction.asJsonObject());
              } else {
                log.error("PENDING Action " + action.toString() + "Received by NQEProcessor but cannot be handled");
              }
              break;
            case INPROCESS:
              log.error("INPROCESS Action " + action.toString() + "Received by NQEProcessor but cannot be handled");
              break;
            case FAILED:
              if (action.getLabel().equals(NQEConstants.DEPLOY_OPERATOR)) {
                log.info("Operator: " + action.getData().getString("operatorType") + " failed");
                com.sendTo(logic, action.asJsonObject());
                activeActions.remove(action.getId());
              } else if (action.getLabel().equals(NQEConstants.DEPLOY_REMOTE_OPERATOR)) {
                Action replyAction = new Action(action.getData());
                String coordinator = action.getData().getString("coordinator");
                replyAction.getData().putString("microcloud", currentCluster);
                replyAction.getData().putString("STATUS", "FAIL");
                String webaddress = getURIFromGlobal(coordinator);
                try {
                  WebServiceClient.completeMapReduce(replyAction.asJsonObject(), webaddress);
                } catch (IOException e) {
                  e.printStackTrace();
                }
                log.error("Remote DEPLOY of " + action.getId() + " failed");
              } else {
                log.error("FAILED Action " + action.toString() + "Received by NQEProcessor but cannot be handled");
              }
              break;
            default:
              break;

          }
        }

      }
    };
    bus = vertx.eventBus();
    config = container.config();
    globalConfig = config.getObject("global");

    id = config.getString("id");
    gr = config.getString("group");
    logic = config.getString("logic");
    workqueue = config.getString("workqueue");
    com = new DefaultNode();
    com.initialize(id, gr, null, leadsHandler, leadsHandler, vertx);
    bus.registerHandler(id + ".process", this);
    LQPConfiguration.initialize();
    LQPConfiguration.getInstance().getConfiguration().setProperty("node.current.component", "nqe");

    String publicIP = ConfigurationUtilities
        .getPublicIPFromGlobal(LQPConfiguration.getInstance().getMicroClusterName(), globalConfig);
    LQPConfiguration.getInstance().getConfiguration().setProperty(StringConstants.PUBLIC_IP, publicIP);
    currentCluster = LQPConfiguration.getInstance().getMicroClusterName();
    persistence = InfinispanClusterSingleton.getInstance().getManager();
    /**
     * !!NETTY new code for the new
     */
    boolean useleveld = LQPConfiguration.getInstance().getConfiguration().getBoolean("index.use.leveldb",true);
    if(useleveld){
      IndexManager.initialize(new Properties());
    }else {
      IndexManager.initialize(null);
    }    NettyDataTransport.initialize(globalConfig);
    /**
     * END OF CODE
     */
    jobsCache = (Cache) persistence.getPersisentCache(StringConstants.QUERIESCACHE);
    JsonObject msg = new JsonObject();
    msg.putString("processor", id + ".process");
    log = new LogProxy(config.getString("log"), com);
    handlers = new HashMap<String, ActionHandler>();
    //     ActionHandler pluginHandler = new DeployPluginActionHandler(com, log, persistence, id, globalConfig);
    handlers.put(NQEConstants.DEPLOY_OPERATOR, new OperatorActionHandler(com, log, persistence, id));
    //      handlers.put(NQEConstants.DEPLOY_PLUGIN,pluginHandler );
    //      handlers.put(NQEConstants.UNDEPLOY_PLUGIN,pluginHandler);
    handlers.put(NQEConstants.DEPLOY_REMOTE_OPERATOR,
        new DeployRemoteOpActionHandler(com, log, persistence, id, globalConfig));
    //
    handlers.put(NQEConstants.EXECUTE_MAP_REDUCE_JOB, new ExecuteMapReduceJobActionHandler(com, log, persistence, id));

    handlers.put(IManagerConstants.EXECUTE_MAPREDUCE, new ExecuteMRActionHandler(com, log, persistence, id));
    handlers.put(IManagerConstants.COMPLETED_MAPREDUCE, new CompletedMRActionHandler(com, log, persistence, id));
    handlers.put(IManagerConstants.PUT_OBJECT, new PutObjectActionHandler(com, log, persistence, id));
    handlers.put(IManagerConstants.GET_QUERY_STATUS, new GetQueryStatusActionHandler(com, log, persistence, id));
    handlers.put(IManagerConstants.STOP_CACHE, new StopCacheActionHandler(com, log, persistence, id, globalConfig));
    handlers.put(IManagerConstants.ADD_LISTENER, new AddListenerActionHandler(com, log, persistence, id, globalConfig));
    handlers.put(IManagerConstants.REMOVE_LISTENER,
        new RemoveListenerActionHandler(com, log, persistence, id, globalConfig));
    bus.send(workqueue + ".register", msg, new Handler<Message<JsonObject>>() {
      @Override public void handle(Message<JsonObject> event) {
        log.info(id + " Registration " + event.address().toString());
      }
    });

    log.info(id + " started ....");
  }

  private String getURIFromGlobal(String coordinator) {
    System.err.println(
        "IN NQE getting URI from global for " + coordinator + " while " + globalConfig.getObject("microclouds`"));
    String uri = globalConfig.getObject("microclouds").getArray(coordinator).get(0);

    if (!uri.startsWith("http:")) {
      uri = "http://" + uri;
    }
    try {
      String portString = uri.substring(uri.lastIndexOf(":") + 1);
      int port = Integer.parseInt(portString);
    } catch (Exception e) {
      log.error("Parsing port execption " + e.getMessage());
      System.err.println("Parsing port execption " + e.getMessage());
      if (uri.endsWith(":")) {
        uri = uri + "8080";
      } else {
        uri = uri + ":8080";
      }

    }
    return uri;
  }

  @Override public void handle(Message<JsonObject> message) {
    try {
      JsonObject body = message.body();
      if (body.containsField("type")) {
        if (body.getString("type").equals("action")) {
          Action action = new Action(body);
          action.setGlobalConf(globalConfig);
          ActionHandler ac = handlers.get(action.getLabel());
          Action result = ac.process(action);
          result.setStatus(ActionStatus.COMPLETED.toString());
          com.sendTo(logic, result.asJsonObject());
          message.reply();
        }
      } else {
        log.error(id + " received message from eventbus that does not contain type field  \n" + message.toString());
      }
    } catch (Exception e) {
      e.printStackTrace();
      log.error(e.getClass().toString());
      log.error(e.getMessage());
    }
  }
}
