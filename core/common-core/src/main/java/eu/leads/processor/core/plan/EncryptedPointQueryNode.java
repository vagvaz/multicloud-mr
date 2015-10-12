package eu.leads.processor.core.plan;

import com.google.gson.annotations.Expose;
import org.apache.tajo.plan.logical.LogicalRootNode;

/**
 * Created by vagvaz on 10/2/14.
 */
public class EncryptedPointQueryNode extends LogicalRootNode {

  @Expose String token;
  @Expose String cache;

  public EncryptedPointQueryNode(int pid) {
    super(pid);
  }

  public String getCache() {
    return cache;
  }

  public void setCache(String cache) {
    this.cache = cache;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }
}
