package eu.leads.processor.nqe.handlers;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.ActionHandler;
import eu.leads.processor.core.ActionStatus;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.core.plan.QueryState;
import eu.leads.processor.core.plan.QueryStatus;
import eu.leads.processor.infinispan.operators.Operator;
import eu.leads.processor.nqe.NQEConstants;

import org.infinispan.Cache;
import org.vertx.java.core.json.JsonObject;

import java.util.List;
import java.util.UUID;

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

  public DeployRemoteOpActionHandler(Node com, LogProxy log, InfinispanManager persistence,
                                     String id,
                                     JsonObject globalConfig) {
    this.com = com;
    this.log = log;
    this.persistence = persistence;
    this.id = id;
  }

  @Override
  public Action process(Action action) {
    Action result = new Action(action.getData().getObject("data"));
    result.setLabel(NQEConstants.DEPLOY_REMOTE_OPERATOR);
    result.getData().putString("owner", id);
//    String jobId = UUID.randomUUID().toString();
//    result.getData().getObject("operator").putString("id", jobId);
//    jobsCache.put(jobId, queryStatus.toString());
//    result.setResult(queryStatus.asJsonObject());
    result.setStatus(ActionStatus.COMPLETED.toString());

    // TODO(ap0n): Maybe use OperatorFactory (and encompass MapReduceOperatorFactory's functionality
    //             there.
    //             - Won't proceed with this (at least for now)as OperatorFactory uses
    //               "operatorType" as a flag while MapReduceOperatorFactory uses job's "name"
    Operator operator = MapReduceOperatorFactory.createOperator(com, persistence, log, result);
    if (operator != null) {
      operator.init(result.getData());
      operator.execute();
    } else {
      log.error("Could not get a valid operator to execute so operator FAILED");
    }
    return result;
  }
}
