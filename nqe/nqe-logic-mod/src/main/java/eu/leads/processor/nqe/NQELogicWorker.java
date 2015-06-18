package eu.leads.processor.nqe;

import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.ActionStatus;
import eu.leads.processor.core.comp.LeadsMessageHandler;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.DefaultNode;
import eu.leads.processor.core.net.MessageUtils;
import eu.leads.processor.core.net.Node;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.UUID;

import static eu.leads.processor.core.ActionStatus.PENDING;

/**
 * Created by vagvaz on 8/4/14.
 */
public class NQELogicWorker extends Verticle implements LeadsMessageHandler {

    private final String componentType = "nqe";
    JsonObject config;
    String monitor;
    String nqeGroup;
    LogProxy log;
    Node com;
    String id;
    String workQueueAddress;
    String currentCluster;
    @Override
    public void start() {
        super.start();
        LQPConfiguration.initialize();
        config = container.config();
        monitor = config.getString("monitor");
        nqeGroup = config.getString("nqe");
        workQueueAddress = config.getString("workqueue");
        id = config.getString("id");
        com = new DefaultNode();
        com.initialize(id, nqeGroup, null, this, null, vertx);
        log = new LogProxy(config.getString("log"), com);


    }

    @Override
    public void stop() {
        super.stop();
        com.unsubscribeFromAll();
    }

    @Override
    public void handle(JsonObject msg) {
        String type = msg.getString("type");
        String from = msg.getString(MessageUtils.FROM);
        String to = msg.getString(MessageUtils.TO);


        if (type.equals("action")) {
            Action action = new Action(msg);
            String label = action.getLabel();
            Action newAction = null;
            action.setProcessedBy(id);
//         action.setStatus(ActionStatus.INPROCESS.toString());

            switch (ActionStatus.valueOf(action.getStatus())) {
                case PENDING: //probably received an action from an external source
                    if (label.equals(NQEConstants.DEPLOY_OPERATOR)) {
                        action.getData().putString("replyTo", action.getData().getString("monitor"));
                        action.setStatus(ActionStatus.INPROCESS.toString());
                        com.sendWithEventBus(workQueueAddress, action.asJsonObject());
                    }else if( (label.equals(NQEConstants.DEPLOY_PLUGIN)) || (label.equals(NQEConstants.UNDEPLOY_PLUGIN))){
                      action.setStatus(ActionStatus.INPROCESS.toString());
                      com.sendWithEventBus(workQueueAddress, action.asJsonObject());
                    }else if(label.equals(NQEConstants.DEPLOY_REMOTE_OPERATOR)){
                      System.err.println("RECEVEIVED REMOTE DEPLOY!!!!!!!!!!");
                      action.setStatus(ActionStatus.INPROCESS.toString());
                      com.sendWithEventBus(workQueueAddress,action.asJsonObject());
                    }

                    else {
                        log.error("Unknown PENDING Action received " + action.toString());
                        return;
                    }
                    action.setStatus(ActionStatus.INPROCESS.toString());
                    if (newAction != null) {
                        action.addChildAction(newAction.getId());
                        logAction(newAction);
                    }
                    logAction(action);
                    break;
                case INPROCESS: //  probably received an action from internal source (processors)
                case COMPLETED: // the action either a part of a multistep workflow (INPROCESSING) or it could be processed.
                    if (label.equals(NQEConstants.DEPLOY_OPERATOR)) {
                        com.sendTo(action.getData().getString("replyTo"), action.getResult());
                    }else if( (label.equals(NQEConstants.DEPLOY_PLUGIN)) || (label.equals(NQEConstants.UNDEPLOY_PLUGIN))){
//                      action.setStatus(ActionStatus.INPROCESS.toString());
//                      com.sendWithEventBus((workQueueAddress,action.asJsonObject());
                    }else if(label.equals(NQEConstants.DEPLOY_REMOTE_OPERATOR)){
                      newAction = new Action(action);
                      newAction.setData(action.getData());
                      newAction.getData().putString("microcloud", currentCluster);
                      newAction.getData().putString("STATUS","SUCCESS");
                    }

                    else {
                        log.error("Unknown COMPLETED OR INPROCESS Action received " + action.toString());
                        return;
                    }
                    finalizeAction(action);
            }
        }
    }

    private void finalizeAction(Action action) {
        //TODO
        //1 inform monitor about completion (if it is completed in this logic each action requires 1 step processing
        //2 remove from processing if necessary
        //3 update action to persistence service
    }


    private void logAction(Action action) {
        //TODO
        //1 inform monitor about action.
        //2 add action to processing set
        //3 update action to persistence service
    }

    private Action createNewAction(Action action) {
        Action result = new Action();
        result.setId(UUID.randomUUID().toString());
        result.setTriggered(action.getId());
        result.setComponentType(componentType);
        result.setStatus(PENDING.toString());
        result.setTriggers(new JsonArray());
        result.setOwnerId(this.id);
        result.setProcessedBy("");
        result.setDestination("");
        result.setData(new JsonObject());
        result.setResult(new JsonObject());
        result.setLabel("");
        result.setCategory("");
        return result;
    }

}
