package eu.leads.processor.core.plan;

import org.vertx.java.core.json.JsonObject;

/**
 * Created by vagvaz on 9/21/14.
 */
public class PhysicalPlanNode extends PlanNode {

  public PhysicalPlanNode() {
    super();
    data.putString("execType", "single");
  }

  public String getExecType() {
    return data.getString("execType");
  }

  public void setExecType(String execType) {
    data.putString("execType", execType);
  }

  public JsonObject preProcess() {
    return data.getObject("preprocess");
  }

  public void setPreProcess(JsonObject preProcess) {
    data.putObject("preprocess", preProcess);
  }

  public void setPostProcess(JsonObject stage) {
    data.putObject("postprocess", stage);
  }

  public JsonObject postProcess() {
    return data.getObject("postprocess");
  }

  public JsonObject onProcess() {
    return data.getObject("onprocess");
  }

  public void setOnProcess(JsonObject stage) {
    data.putObject("onprocess", stage);
  }
}
