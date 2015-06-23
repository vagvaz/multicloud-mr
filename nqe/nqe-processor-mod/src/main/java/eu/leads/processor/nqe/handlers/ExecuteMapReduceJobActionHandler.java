package eu.leads.processor.nqe.handlers;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.ActionHandler;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;

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
    // TODO(ap0n): Understand and complete this method
    Action result = new Action();
    return result;
  }
}
