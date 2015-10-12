package eu.leads.processor.core.plan;

import org.apache.tajo.plan.logical.LogicalRootNode;
import org.vertx.java.core.json.JsonObject;

import java.util.Collection;

/**
 * Created by vagvaz on 8/4/14.
 */
public interface Plan {
  public PlanNode getOutput();

  public void setOutput(PlanNode node);

  //   public void addSource(PlanNode node);
  //
  //   public void addTo(String node, Plan subPlan) throws Exception;
  //
  //   public void addTo(String node, PlanNode subnode) throws Exception;
  //
  //   public void addAfter();
  //
  //   public void addAfterCurrent(PlanNode node) throws Exception;
  //
  //   public void addAfterCurrent();
  //
  //   public void addToCurrent(PlanNode node) throws Exception;
  //
  //   public void addToCurrent();


  //   public void merge(Plan extracted) throws Exception;

  public Collection<PlanNode> getNodes();

  //   public PlanNode getCurrent();

  //   public void setCurrent(String nodeId) throws Exception;
  //
  //   public void setCurrent(PlanNode node) throws Exception;

  public PlanNode getNode(String nodeId);

  public Collection<String> getSources();

  public JsonObject getRootNode();

  public void setRootNode(JsonObject rootNode);

  public void setRootNode(LogicalRootNode rootNode);

  public JsonObject getPlanGraph();

  public void setPlanGraph(JsonObject planGraph);

  public String getQueryId();

  public void setQueryId(String queryId);

  public void addParentTo(String nodeId, PlanNode newNode);

  public void addChildTo(String nodeId, PlanNode newNode);

  public JsonObject getNodeById(String id);

  public JsonObject getNodeByPid(int pid);



}
