package eu.leads.processor.nqe.handlers;

import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.ActionHandler;
import eu.leads.processor.core.ActionStatus;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;

import org.infinispan.Cache;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by Apostolos Nydriotis on 2015/06/24.
 */
public class GetQueryStatusActionHandler implements ActionHandler {

  Node com;
  LogProxy log;
  InfinispanManager persistence;
  String id;
  Cache<String, String> queriesCache;

  public GetQueryStatusActionHandler(Node com, LogProxy log, InfinispanManager persistence,
                                     String id) {
    this.com = com;
    this.log = log;
    this.persistence = persistence;
    this.id = id;
    queriesCache = (Cache) persistence.getPersisentCache(StringConstants.QUERIESCACHE);
  }

  @Override
  public Action process(Action action) {
    Action result = action;
    JsonObject actionResult = new JsonObject();
    try {
      String queryId = action.getData().getString("queryId");
//            JsonObject actionResult = persistence.get(StringConstants.QUERIESCACHE, queryId);
      log.info("read query");
      String queryJson = queriesCache.get(queryId);

      JsonObject query = new JsonObject(queryJson);
      result.setResult(query);

    } catch (Exception e) {
      log.info("exception in read");
      actionResult.putString("error", e.getMessage());
      result.setResult(actionResult);
      e.printStackTrace();
    }
    result.setStatus(ActionStatus.COMPLETED.toString());
    log.info("preturn query status");
    return result;
  }
}
