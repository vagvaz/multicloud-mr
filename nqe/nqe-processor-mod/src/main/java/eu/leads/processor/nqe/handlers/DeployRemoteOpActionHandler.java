package eu.leads.processor.nqe.handlers;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.ActionHandler;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.infinispan.operators.Operator;
import eu.leads.processor.nqe.NQEConstants;
import org.infinispan.Cache;
import org.vertx.java.core.json.JsonObject;

import java.util.List;

/**
 * Created by vagvaz on 4/20/15.
 */
public class DeployRemoteOpActionHandler implements ActionHandler {
  private final Node com;
  private final LogProxy log;
  private final InfinispanManager persistence;
  private final String id;


  private String textFile;
  private transient Cache<?, ?> InCache;
  private transient Cache<?, List<?>> CollectorCache;
  private transient Cache<?, ?> OutCache;

  public DeployRemoteOpActionHandler(Node com, LogProxy log, InfinispanManager persistence, String id,
                                      JsonObject globalConfig){
  this.com = com;
  this.log = log;
  this.persistence = persistence;
  this.id = id;
}

  @Override
  public Action process(Action action) {
    Action result = new Action(action.getData().getObject("data"));
    result.getData().putString("owner",id);
    result.setLabel(NQEConstants.DEPLOY_REMOTE_OPERATOR);
    Action ownerAction = new Action(result.asJsonObject().copy());
//    ownerAction.setLabel(NQEConstants.OPERATOR_OWNER);
//    ownerAction.setStatus(ActionStatus.INPROCESS.toString());
//    com.sendTo(action.getData().getString("monitor"),ownerAction.asJsonObject());
    Operator operator = OperatorFactory.createOperator(com,persistence,log,result);
    if(operator != null) {
      operator.init(result.getData());
      operator.execute();
    }
    else{
      log.error("Could not get a valid operator to execute so operator FAILED");
      ownerAction.setLabel(NQEConstants.OPERATOR_FAILED);
      com.sendTo(action.getData().getString("monitor"),ownerAction.asJsonObject());
    }

    return result;
  }
}
