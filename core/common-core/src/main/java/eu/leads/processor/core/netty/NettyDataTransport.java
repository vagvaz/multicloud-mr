package eu.leads.processor.core.netty;

import io.netty.channel.Channel;
import org.vertx.java.core.json.JsonObject;

/**
 * This class is used to actually transfer/receive data from other nodes.
 * Thus, an array of connections can be used (probably netty has another way of defining sth like that, or we could
 * not use netty if it makes it harder.
 * This is a rough design for this class the API can be changed  however we like it and find it more convenient.
 * Probably a good example for what we will need is this
 * http://netty.io/4.1/xref/io/netty/example/udt/echo/bytes/package-summary.html
 * Created by vagvaz on 10/21/15.
 */
public class NettyDataTransport {
  static JsonObject globalConfiguration;

  /**
   * Using the json object we initialize the connections, in particular we use the componentAddrs data the IPs withing
   * a microcloud are separated by ; and the microclouds are separated by |
   * probably connections should be started with the respective netty handlers as well as the servers. In case of errors,
   * for example when a node is done the action should be retried (inside each Thread should we use a more socket like approach)
   * This way the system will boot up otherwise we might get into deadlock.
   * If we take a more netty approach we could start the server here and let the NettyKeyValueDataTransfer class to initialize
   * each client or we could initialize each client lazily when first used (probably the best action)
   * Furthermore, from the netty examples I have read, I think we also need to initialize the receiver handlers in order to
   * be able to handle incoming data.
   *
   * @param globalConfiguration
   */
  public static void initialize(JsonObject globalConfiguration) {
  }

  /**
   * The method is called to send data to another node of course if the put is local then the data are directly put into
   * The queue for the respective index.
   * functionality pseudoCode
   * if(channel.equals( me))
   * IndexManager.put(indexName,key,value);
   * else
   * Message msg = new NettyMessage (indexName,key,value) this class is Serializable)
   * channel.write(msg)
   * The actual implementation should be a bit more comples as ( if netty does not already does that each connection should
   * have its own thread. As a by product of this thre might be a need for an individual queue.
   *
   * @param channel
   * @param indexName
   * @param key
   * @param value
   */
  public static void send(Channel channel, String indexName, Object key, Object value) {

  }

  /**
   * We might use this method to get all the necessary configuration we might need for initializing NettyKeyValueDataTransfer
   */

  public static JsonObject getGlobalConfig() {
    return globalConfiguration;
  }



}
