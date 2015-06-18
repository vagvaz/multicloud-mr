package eu.leads.processor.core;

import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.comp.LeadsMessageHandler;
import eu.leads.processor.core.net.DefaultNode;
import eu.leads.processor.core.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by vagvaz on 8/11/14.
 */
public class LogSink extends Verticle implements LeadsMessageHandler {

    String id;
    Set<String> groups;
    String configuration;
    Node com;
    Logger logger = LoggerFactory.getLogger(LogSink.class.getName());

    @Override
    public void start() {
        super.start();
        JsonObject config = container.config();
        id = config.getString("id");
        JsonArray logGroups = config.getArray("groups");
        Iterator<Object> iterator = logGroups.iterator();
        groups = new HashSet<String>();
        while (iterator.hasNext()) {
            String g = (String) iterator.next();
            groups.add(g);
        }
        configuration = config.getString("configuration");
        LQPConfiguration.initialize(true);
        //      LQPConfiguration.parseFile(configuration);
        com = new DefaultNode();
        com.initialize(id, "leads.log.sink", groups, this, this, vertx);

    }

    @Override
    public void handle(JsonObject msg) {
        String type = msg.getString("type");
        String message = msg.getString("message");
        String component = msg.getString("component");
        if (type.equals("info")) {
            logger.info(component + ": " + message);


        } else if (type.equals("warn")) {
            logger.warn(component + ": " + message);
            //         bus.sendToAllGroup("eu.leads.processor.log.warn", msg);
        } else if (type.equals("error")) {
            logger.error(component + ": " + message);

            //         bus.sendToAllGroup("eu.leads.processor.log.error", msg);
        } else if (type.equals("fatal")) {
            logger.error(component + ": FATAL " + message);
            //         bus.sendToAllGroup("eu.leads.processor.log.fatal", msg);
        } else {
            logger.debug(component + ": " + message);

            //         bus.sendToAllGroup("eu.leads.processor.log.debug", msg);
        }
    }
}
