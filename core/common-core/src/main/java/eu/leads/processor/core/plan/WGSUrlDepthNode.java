package eu.leads.processor.core.plan;

import com.google.gson.annotations.Expose;
import org.apache.tajo.plan.logical.LogicalRootNode;

/**
 * Created by vagvaz on 9/8/14.
 */
public class WGSUrlDepthNode extends LogicalRootNode {
  @Expose String url;
  @Expose int depth;

  public WGSUrlDepthNode(int pid) {
    super(pid);
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) {
    this.depth = depth;
  }

  public void setDepth(String depthAsString) {
    this.depth = Integer.parseInt(depthAsString);
  }
}
