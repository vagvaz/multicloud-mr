package eu.leads.processor.web;

import com.google.common.base.Strings;
import eu.leads.processor.common.StringConstants;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.ActionStatus;
import eu.leads.processor.core.comp.LeadsMessageHandler;
import eu.leads.processor.core.net.MessageUtils;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.imanager.IManagerConstants;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by vagvaz on 8/4/14.
 */
public class GetResultsHandler implements Handler<HttpServerRequest> {

    Node com;
    Logger log;
    Map<String, GetResultsReplyHandler> replyHandlers;


    public GetResultsHandler(final Node com, Logger log) {
        this.com = com;
        this.log = log;
        replyHandlers = new HashMap<>();
    }

    @Override
    public void handle(HttpServerRequest request) {
        request.response().setStatusCode(200);
        request.response().putHeader(WebStrings.CONTENT_TYPE, WebStrings.APP_JSON);
        log.info("Put Object Request");
        String reqId = UUID.randomUUID().toString();
        GetResultsReplyHandler replyHandler = new GetResultsReplyHandler(reqId, request);
        JsonObject queryRequest = new JsonObject();
        queryRequest.putString("type", "getResults");
        String queryId = request.params().get("id");
        String min = request.params().get("min");
        String max = request.params().get("max");
        if (Strings.isNullOrEmpty(queryId)) {
            replyHandler.replyForError(null);
            return;
        }

        queryRequest.putString("queryId", queryId);
        queryRequest.putString("min", min);
        queryRequest.putString("max", max);


        Action action = new Action();
        action.setId(reqId);
        action.setCategory(StringConstants.ACTION);
        action.setLabel(IManagerConstants.GET_RESULTS);
        action.setOwnerId(com.getId());
        action.setComponentType("webservice");
        action.setTriggered("");
        action.setTriggers(new JsonArray());
        action.setDestination(StringConstants.IMANAGERQUEUE);
        action.setStatus(ActionStatus.PENDING.toString());
        action.setData(queryRequest);

        com.sendRequestTo(StringConstants.IMANAGERQUEUE, action.asJsonObject(), replyHandler);
        replyHandlers.put(reqId, replyHandler);
    }

    public void cleanup(String id) {
        GetResultsReplyHandler rh = replyHandlers.remove(id);
        rh = null;
    }


    private class GetResultsReplyHandler implements LeadsMessageHandler {
        HttpServerRequest request;
        String requestId;

        public GetResultsReplyHandler(String requestId, HttpServerRequest request) {
            this.request = request;
            this.requestId = requestId;
        }

        @Override
        public void handle(JsonObject message) {
            if (message.containsField("error")) {
                replyForError(message);
                return;
            }
            log.info("GetResults webservice received reply " + message.getString(MessageUtils.TO) + " " + message.getValue(MessageUtils.MSGID).toString());
            message.removeField(MessageUtils.FROM);
            message.removeField(MessageUtils.TO);
            message.removeField(MessageUtils.COMTYPE);
            message.removeField(MessageUtils.MSGID);
            message.removeField(MessageUtils.MSGTYPE);
//            log.info("end requests");
            request.response().end(message.toString());
            cleanup(requestId);
        }

        private void replyForError(JsonObject message) {
            if (message != null) {
                log.error(message.getString("message"));
                request.response().end("{}");
            } else {
                log.error("Request for Query Status had empty query Id.");
                request.response().setStatusCode(400);
            }
            cleanup(requestId);
        }
    }
}

