package eu.leads.processor.infinispan.operators;

import eu.leads.processor.core.Action;
import eu.leads.processor.core.comp.LeadsMessageHandler;
import eu.leads.processor.core.net.Node;
import org.vertx.java.core.json.JsonObject;

import java.util.Set;

/**
 * Created by vagvaz on 4/16/15.
 */
public class CompleteExecutionHandler implements LeadsMessageHandler {

   private Node com;
   private BasicOperator operator;
   public CompleteExecutionHandler(Node com, BasicOperator operator){
      this.com = com;
      this.operator = operator;
   }
   @Override
   public void handle(JsonObject message) {
      Action action = new Action(message);
      JsonObject data = action.getData();
      operator.addResult(data.getString("microcloud"),data.getString("STATUS"));
      operator.signal();
   }
}
