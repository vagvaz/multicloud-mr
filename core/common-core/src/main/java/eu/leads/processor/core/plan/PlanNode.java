package eu.leads.processor.core.plan;

import eu.leads.processor.core.DataType;
import org.apache.tajo.plan.logical.LogicalNode;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by vagvaz on 8/4/14.
 */
public class PlanNode extends DataType {

  public PlanNode() {
    super();
    setSite("C&H micro-cloud 0");
  }

  public PlanNode(JsonObject node) {
    super(node);
    if (!node.containsField("status"))
      setStatus(NodeStatus.PENDING);
    if (!node.containsField("site")) {
      setSite("C&H micro-cloud 0");
    }

  }

  public PlanNode(LogicalNode n) {
    super();
    JsonObject configuration = new JsonObject(n.toJson());
    this.setConfiguration(configuration);
    setNodeType(LeadsNodeType.valueOf(configuration.getString("type")));
    setStatus(NodeStatus.PENDING);
    setSite("C&H micro-cloud 0");
  }

  public PlanNode(LogicalNode n, String queryId) {
    super();
    JsonObject configuration = new JsonObject(n.toJson());
    this.setConfiguration(configuration);
    setNodeType(LeadsNodeType.valueOf(configuration.getString("type")));
    generateId(queryId);
    setStatus(NodeStatus.PENDING);
    setSite("C&H micro-cloud 0");
  }

  public LeadsNodeType getNodeType() {
    return LeadsNodeType.valueOf(data.getString("nodetype"));
  }

  public void setNodeType(LeadsNodeType nodeType) {
    data.putString("nodetype", nodeType.toString());
  }

  public List<String> getInputs() {
    ArrayList<String> result = new ArrayList<String>();
    JsonArray array = data.getArray("inputs");
    if (array != null && array.size() > 0) {
      Iterator<Object> inputIterator = array.iterator();
      while (inputIterator.hasNext()) {
        result.add((String) inputIterator.next());
      }
    }
    return result;
  }

  public void setInputs(List<String> inputs) {
    JsonArray array = new JsonArray();
    for (String input : inputs) {
      array.add(input);
    }
    data.putArray("inputs", array);
  }

  public void addInput(String input) {
    JsonArray array = data.getArray("inputs");
    if (array == null)
      array = new JsonArray();
    array.add(input);
    data.putArray("inputs", array);
  }

  public String getOutput() {
    String result = data.getString("output");
    return result;
  }

  public void setOutput(String output) {
    data.putString("output", output);
  }

  public JsonObject getConfiguration() {
    JsonObject result = data.getObject("configuration");
    return result;
  }

  public void setConfiguration(JsonObject configuration) {
    data.putObject("configuration", configuration);
  }

  public Integer getPid() {
    String result = "";
    JsonObject conf = getConfiguration();
    return conf.getObject("body").getInteger("nodeId");
  }

  public String getNodeId() {
    return data.getString("id");
  }

  public void setId(String id) {
    data.putString("id", id);
  }

  public void generateId(String prefix) {
    Integer pid = getPid();
    data.putString("id", prefix + "." + pid.toString());
  }

  public Double getKParameter() {
    return (Double) data.getNumber("k");
  }

  public void setKParameter(double k) {
    data.putNumber("k", k);
  }

  public Double getVParameter() {
    return (Double) data.getNumber("v");
  }

  public void setVParameter(double v) {
    data.putNumber("v", v);
  }

  public JsonObject getSchedulerRepresentation() {
    return null;
  }


  public NodeStatus getStatus() {
    return NodeStatus.valueOf(data.getString("status", NodeStatus.UNKNOWN.toString()));
  }

  public void setStatus(NodeStatus status) {
    data.putString("status", status.toString());
  }

  public String getSite() {
    return data.getString("site");
  }

  public void setSite(String site) {
    data.putString("site", site);
  }
}
