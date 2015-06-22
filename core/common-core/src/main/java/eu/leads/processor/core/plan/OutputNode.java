package eu.leads.processor.core.plan;

import org.apache.tajo.plan.logical.LogicalRootNode;

/**
 * Created by vagvaz on 8/29/14.
 */
public class OutputNode extends LogicalRootNode {

  public OutputNode(int pid) {
    super(pid);
  }

  LeadsNodeType getNodeType() {
    return LeadsNodeType.OUTPUT_NODE;
  }
}
