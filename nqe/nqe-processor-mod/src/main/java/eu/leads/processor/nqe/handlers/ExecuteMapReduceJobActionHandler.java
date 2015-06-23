package eu.leads.processor.nqe.handlers;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.ActionHandler;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.infinispan.operators.Operator;
import eu.leads.processor.nqe.NQEConstants;

/**
 * Created by Apostolos Nydriotis on 2015/06/22.
 */
public class ExecuteMapReduceJobActionHandler implements ActionHandler {

  private final Node com;
  private final LogProxy log;
  private final InfinispanManager persistence;
  private final String id;

  public ExecuteMapReduceJobActionHandler(Node com, LogProxy log, InfinispanManager persistence,
                                          String id) {
    this.com = com;
    this.log = log;
    this.persistence = persistence;
    this.id = id;
  }

  @Override
  public Action process(Action action) {
    Action result = new Action(action.getData().getObject("data"));
    result.getData().putString("owner", id);
    result.setLabel(NQEConstants.EXECUTE_MAP_REDUCE_JOB);
    Action ownerAction = new Action(result.asJsonObject().copy());
    // TODO(ap0n): What's ownerAction? Should it be used here?

    // TODO(ap0n): Maybe use OperatorFactory (and encompass MapReduceOperatorFactory's functionality
    //             there.
    Operator operator = MapReduceOperatorFactory.createOperator(com, persistence, log, result);
    if (operator != null) {
      operator.init(result.getData());
      operator.execute();
    } else {
      log.error("Could not get a valid operator to execute so operator FAILED");
      // TODO(ap0n): What's the "monitor"?
      com.sendTo(action.getData().getString("monitor"), ownerAction.asJsonObject());
    }

    return result;
  }
}
