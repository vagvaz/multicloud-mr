package eu.leads.processor.core.plan;

import eu.leads.processor.core.DataType;
import org.apache.tajo.plan.logical.*;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by vagvaz on 8/4/14.
 */
public class SQLPlan extends DataType implements Plan {
  public SQLPlan(JsonObject plan) {
    super(plan);
  }

  public SQLPlan() {
    super();
  }

  public SQLPlan(String queryId) {
    super();
    setQueryId(queryId);
  }

  public SQLPlan(LogicalRootNode rootNode) {
    super();
    setQueryId("no-query-id-defined");
    computeInternalStructures(rootNode, getQueryId());
  }

  public SQLPlan(String queryId, LogicalRootNode rootNode) {
    super();
    setQueryId(queryId);
    computeInternalStructures(rootNode, queryId);
  }

  private void computeInternalStructures(LogicalRootNode rootNode, String queryId) {
    JsonObject planGraph = generatePlan(rootNode);
    setPlanGraph(planGraph);
    //       System.out.println("PLAN$$\n"+ planGraph.encodePrettily() );
    JsonArray nodes = new JsonArray();
    for (String node : planGraph.getFieldNames()) {
      nodes.add(planGraph.getObject(node));
    }
    data.putArray("nodes", nodes);
    setRootNode(rootNode);
    JsonObject nodesByPid = getNodesAllNodesByPid(rootNode);
    data.putObject("nodesByPID", nodesByPid);


  }

  private JsonObject getNodesAllNodesByPid(LogicalRootNode rootNode) {
    JsonObject result = new JsonObject();
    LogicalNode current = rootNode;
    List<LogicalNode> toProcess = new ArrayList<>();
    toProcess.add(current);
    result.putObject(String.valueOf(current.getPID()), cleanChilds(current));
    while (toProcess.size() > 0) {
      current = toProcess.remove(0);
      if (current instanceof UnaryNode) {
        UnaryNode currentTmp = (UnaryNode) current;
        LogicalNode n = currentTmp.getChild();
        JsonObject toadd = cleanChilds(n);
        result.putObject(String.valueOf(n.getPID()), toadd);
        toProcess.add(n);
      } else if (current instanceof BinaryNode) {
        BinaryNode currentTmp = (BinaryNode) current;
        LogicalNode l = currentTmp.getLeftChild();
        LogicalNode r = currentTmp.getRightChild();
        JsonObject toaddLeft = cleanChilds(l);
        result.putObject(String.valueOf(l.getPID()), toaddLeft);
        toProcess.add(l);
        JsonObject toaddRight = cleanChilds(r);
        result.putObject(String.valueOf(r.getPID()), toaddRight);
        toProcess.add(r);

      } else if (current instanceof RelationNode) {
        if (current instanceof ScanNode) {
          result.putObject(String.valueOf(current.getPID()), new JsonObject(current.toJson()));
        } else if (current instanceof TableSubQueryNode) {
          TableSubQueryNode tmp = (TableSubQueryNode) current;
          LogicalNode n = tmp.getSubQuery();
          result.putObject(String.valueOf(n.getPID()), cleanChilds(n));
          toProcess.add(n);
        } else {
          System.err.println("PROBLEM WITH PIDRELNODE TYPES");
        }
      }
    }
    return result;
  }

  private JsonObject cleanChilds(LogicalNode current) {
    JsonObject result = new JsonObject(current.toJson());
    if (current instanceof UnaryNode) {
      result.getObject("body").removeField("child");
    } else if (current instanceof BinaryNode) {

      result.getObject("body").removeField("leftChild");
      result.getObject("body").removeField("rightChild");
    } else {
      if (current instanceof RelationNode) {
        if (current instanceof ScanNode)
          ;
        else if (current instanceof TableSubQueryNode) {
          result.getObject("body").getObject("subQuery").getObject("body").removeField("child");
        } else {
          System.err.println("PROBLEM WITH RELNODE TYPES");
        }
      } else {
        System.err.println("PROBLEM WITH NODE TYPES");
      }

    }
    return result;
  }

  private JsonObject generatePlan(LogicalRootNode rootNode) {
    JsonObject result = new JsonObject();
    PlanNode planNode = new PlanNode(rootNode);
    PlanNode outputNode = new PlanNode();
    outputNode.setOutput("");
    outputNode.setNodeType(LeadsNodeType.OUTPUT_NODE);
    outputNode.setId(getQueryId() + ".output");


    PlanNode top = new PlanNode(rootNode, getQueryId());
    List<PlanNode> toProcess = new ArrayList<>();

    outputNode.addInput(getQueryId() + "." + top.getPid());
    top.setOutput(outputNode.getNodeId());
    this.setOutput(outputNode);

    result.putObject(outputNode.getNodeId(), outputNode.asJsonObject());
    LogicalNode current = rootNode;
    visit(top, current, result);
    return result;
  }

  private void visit(PlanNode top, LogicalNode current, JsonObject result) {
    if (current instanceof UnaryNode) {
      UnaryNode currentTmp = (UnaryNode) current;
      LogicalNode n = currentTmp.getChild();
      PlanNode currentNode = new PlanNode(n, getQueryId());
      top.addInput(currentNode.getNodeId());
      top.asJsonObject().getObject("configuration").getObject("body").removeField("child");
      result.putObject(top.getNodeId(), top.asJsonObject());
      currentNode.setOutput(top.getNodeId());
      visit(currentNode, n, result);
    } else if (current instanceof BinaryNode) {
      BinaryNode currentTmp = (BinaryNode) current;
      LogicalNode l = currentTmp.getLeftChild();
      LogicalNode r = currentTmp.getRightChild();
      PlanNode left = new PlanNode(l, getQueryId());
      PlanNode right = new PlanNode(r, getQueryId());
      top.addInput(left.getNodeId());
      top.addInput(right.getNodeId());
      left.setOutput(top.getNodeId());
      right.setOutput(top.getNodeId());
      if (current instanceof JoinNode) {
        System.out.println("\n\n\n\n\n\n\n\nJOIN\n\n\n\n\n\n\n\n\n" + current.toJson());
        JoinNode joinNode = (JoinNode) current;
        System.out.println("\n\n\n\n\n\n\n\nJOIN\n\n\n\n\n\n\n\n\n" + current.toJson());
        LogicalNode leftNode = joinNode.getLeftChild();
        LogicalNode rightNode = joinNode.getRightChild();
        JsonObject leftSchema = new JsonObject(leftNode.getOutSchema().toJson());
        JsonObject rightSchema = new JsonObject(rightNode.getOutSchema().toJson());
        top.asJsonObject().getObject("configuration").getObject("body").putObject("leftSchema", leftSchema);
        top.asJsonObject().getObject("configuration").getObject("body").putObject("rightSchema", rightSchema);

      }
      top.asJsonObject().getObject("configuration").getObject("body").removeField("leftChild");
      top.asJsonObject().getObject("configuration").getObject("body").removeField("rightChild");
      result.putObject(top.getNodeId(), top.asJsonObject());
      visit(left, l, result);
      visit(right, r, result);
    } else {
      if (current instanceof RelationNode) {
        if (current instanceof ScanNode) {
          ScanNode sc = (ScanNode) current;
          top.addInput(sc.getTableDesc().getName());
          result.putObject(top.getNodeId(), top.asJsonObject());
        } else if (current instanceof TableSubQueryNode) {
          TableSubQueryNode tmp = (TableSubQueryNode) current;
          LogicalNode n = tmp.getSubQuery();
          PlanNode currentNode = new PlanNode(n, getQueryId());
          top.addInput(currentNode.getNodeId());
          top.asJsonObject().getObject("configuration").getObject("body").getObject("subQuery").getObject("body")
              .removeField("child");
          result.putObject(top.getNodeId(), top.asJsonObject());
          currentNode.setOutput(top.getNodeId());
          visit(currentNode, n, result);
        } else {
          System.err.println("PROBLEM WITH RELNODE TYPES");
        }
      } else if (current instanceof EvalExprNode) {
        EvalExprNode tmp = (EvalExprNode) current;
        PlanNode currentNode = new PlanNode(tmp, getQueryId());
        currentNode.addInput("");
        currentNode.setOutput(top.getOutput());
        result.putObject(currentNode.getNodeId(), currentNode.asJsonObject());
        setIsSpecial(true);
      } else {
        System.err.println("PROBLEM WITH NODE TYPES");
      }

    }
  }

  @Override public PlanNode getOutput() {
    PlanNode result = new PlanNode(data.getObject("output"));
    return result;
  }

  @Override public void setOutput(PlanNode node) {
    data.putObject("output", node.asJsonObject());
  }

  @Override public Collection<PlanNode> getNodes() {
    JsonArray nodes = data.getArray("nodes");
    List<PlanNode> result = new ArrayList<>();
    Iterator<Object> it = nodes.iterator();
    while (it.hasNext()) {
      result.add(new PlanNode((JsonObject) it.next()));
    }
    return result;
  }

  @Override public PlanNode getNode(String nodeId) {
    JsonObject jsonNode = data.getObject("plan").getObject(nodeId);
    if (jsonNode == null) {
      return null;
    }
    PlanNode result = new PlanNode(jsonNode);
    return result;
  }

  @Override public Collection<String> getSources() {
    JsonArray sources = data.getArray("sources");
    List<String> result = new ArrayList<>();
    Iterator<Object> it = sources.iterator();
    while (it.hasNext()) {
      result.add((String) it.next());
    }
    return result;
  }

  @Override public JsonObject getRootNode() {
    return data.getObject("rootNode");
  }

  @Override public void setRootNode(JsonObject rootNode) {
    ;
  }

  @Override public void setRootNode(LogicalRootNode rootNode) {
    data.putObject("rootNode", new JsonObject(rootNode.toJson()));
  }
  //@Override
  // public void setRootNode(JsonObject rootNode) {
  //    data.putObject("rootNode", rootNode);
  //}



  //    @Override
  //    public void setRootNode(LogicalRootNode rootNode) {
  //        JsonObject jsonObject = new JsonObject(rootNode.toJson());
  //        setRootNode(jsonObject);
  //    }

  @Override public JsonObject getPlanGraph() {
    JsonObject result = data.getObject("plan");
    if (result == null)
      return null;
    return result;
  }

  @Override public void setPlanGraph(JsonObject planGraph) {
    data.putObject("plan", planGraph);
  }

  @Override public String getQueryId() {
    return data.getString("queryId");
  }

  @Override public void setQueryId(String queryId) {
    data.putString("queryId", queryId);
  }

  @Override public void addParentTo(String nodeId, PlanNode newNode) {

  }

  @Override public void addChildTo(String nodeId, PlanNode newNode) {

  }

  @Override public JsonObject getNodeById(String id) {
    JsonObject node = data.getObject("nodesByPID").getObject(id);
    return node;

  }

  @Override public JsonObject getNodeByPid(int pid) {
    return getNodeById(Integer.toString(pid));
  }

  public void setIsSpecial(boolean isSpecial) {
    data.putBoolean("planIsSpecial", true);
  }

  public boolean getIsSpecial() {
    return data.getBoolean("planIsSpecial", false);
  }

  public void updatePlanNode(PlanNode node) {
    data.getObject("plan").putObject(node.getNodeId(), node.asJsonObject());
  }

  public void updateNode(PlanNode node) {
    data.getObject("plan").putObject(node.getNodeId(), node.asJsonObject());
    JsonArray oldNodesArray = data.getArray("nodes");
    JsonArray newNodesArray = new JsonArray();
    Iterator<Object> iterator = oldNodesArray.iterator();
    while (iterator.hasNext()) {
      JsonObject ob = (JsonObject) iterator.next();
      if (ob.getString("id").equals(node.getNodeId())) {
        newNodesArray.add(node.asJsonObject());
      } else {
        newNodesArray.add(ob);
      }
    }
    data.putArray("nodes", newNodesArray);
  }

}
