package eu.leads.processor.core.netty;

import eu.leads.processor.common.infinispan.ClusterInfinispanManager;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.conf.LQPConfiguration;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.jboss.marshalling.serial.Serial;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.impl.ConcurrentHashSet;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
  static EventLoopGroup workerGroup;
  static EventLoopGroup bossGroup;
  static Bootstrap clientBootstrap;
  static ServerBootstrap serverBootstrap;
  static Set<ChannelFuture> channelFutures;
  static Map<String,ChannelFuture> nodes;
  static String me;
  static Map<Channel,Set<Integer>> pending;
  private static NettyClientChannelInitializer clientChannelInitializer;
  private static NettyServerChannelInitializer serverChannelInitializer;
  private static ChannelFuture serverFuture;
  private static AtomicInteger counter = new AtomicInteger(0);
  private static Map<String,Long> histogram;
  private static Logger log = LoggerFactory.getLogger(NettyDataTransport.class);
  private static boolean nodesInitialized = false;
  private static boolean discard = false;

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
    NettyDataTransport.globalConfiguration = globalConfiguration;
    discard = LQPConfiguration.getInstance().getConfiguration().getBoolean("transfer.discard",false);
    clientChannelInitializer = new NettyClientChannelInitializer();
    serverChannelInitializer = new NettyServerChannelInitializer();
    pending = new HashMap<>();
    nodes = new TreeMap<>();
    channelFutures = new HashSet<>();
    histogram = new HashMap<>();

    clientBootstrap = new Bootstrap();
    serverBootstrap = new ServerBootstrap();
    workerGroup = new NioEventLoopGroup();
    bossGroup = new NioEventLoopGroup();
    clientBootstrap.group(workerGroup);
//    clientBootstrap.group(workerGroup);
    clientBootstrap.channel(NioSocketChannel.class);
    clientBootstrap.option(ChannelOption.SO_KEEPALIVE,true).handler(clientChannelInitializer);
    serverBootstrap.group(bossGroup,workerGroup).channel(NioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG,128)
        .option(ChannelOption.SO_REUSEADDR,true)
        .childOption(ChannelOption.SO_KEEPALIVE,true)
        //        .childOption(ChannelOption.SO_RCVBUF,2*1024*1024)
        .childHandler(serverChannelInitializer);
    try {
      serverFuture = serverBootstrap.bind(getPort()).sync();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public synchronized static void initializeNodes(){
    if(nodesInitialized){
      return;
    }
    JsonObject componentAddrs = globalConfiguration.getObject("componentsAddrs");
    List<String> clouds = new ArrayList(componentAddrs.getFieldNames());
    Collections.sort(clouds);
    for(String microCloud : clouds ){
      JsonArray array = componentAddrs.getArray(microCloud);
      String microCloudIPs = array.get(0);
      String[] URIs = microCloudIPs.split(";");
      for(String URI : URIs){
        String[] parts = URI.split(":");
        String host = parts[0];
        String portString = parts[1];
        boolean ok = false;
        while(!ok){
          try {
            ChannelFuture f = clientBootstrap.connect(host,getPort(portString)).sync();

            ok = true;
            nodes.put(host,f);
            pending.put(f.channel(),new ConcurrentHashSet<Integer>(100));
            channelFutures.add(f);
            histogram.put(host,0L);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
    nodesInitialized=true;
  }

  private static int getPort(String portString) {
    Integer result = Integer.parseInt(portString);
    return 10000+(result - 11222);
  }

  private static int getPort() {
    int result = 10000;
    ClusterInfinispanManager clusterInfinispanManager =
        (ClusterInfinispanManager) InfinispanClusterSingleton.getInstance().getManager();
    result += clusterInfinispanManager.getServerPort()-11222;
    return result;
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
    send(channel.remoteAddress().toString(),indexName,key,value);
  }

  public static void send(String name, String indexName, Object key, Object value) {
    Serializable keySerializable = (Serializable)key;
    Serializable valueSerializable = (Serializable)value;
    try {
      ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(byteArray);
      oos.writeObject(key);
      oos.writeObject(value);
      send(name,indexName,byteArray.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
    }

  }


  public static void send(String target, String cacheName, byte[] bytes) {
    NettyMessage nettyMessage = new NettyMessage(cacheName,bytes,getCounter());
    ChannelFuture f = nodes.get(target);
    if(!discard) {
      pending.get(f.channel()).add(nettyMessage.getMessageId());

      updateHistogram(target, bytes);

      f.channel().write(nettyMessage, f.channel().voidPromise());
    }
  }

  private static void updateHistogram(String target, byte[] bytes) {
    Long tmp = histogram.get(target);
    tmp += bytes.length;
    histogram.put(target,tmp);
  }

  private static  int getCounter() {
    //    counter = (counter+1) % Integer.MAX_VALUE;
    //    return counter;
    return counter.addAndGet(1);
  }

  /**
   * We might use this method to get all the necessary configuration we might need for initializing NettyKeyValueDataTransfer
   */

  public static JsonObject getGlobalConfig() {
    return globalConfiguration;
  }




  public static void spillMetricData() {
    for(Map.Entry<String,Long> entry : histogram.entrySet()){
      PrintUtilities.printAndLog(log,"SPILL: " + entry.getKey() + " " + entry.getValue());
    }
  }

  public static void waitEverything() {
    for(Map.Entry<String,ChannelFuture> entry: nodes.entrySet()){
      entry.getValue().channel().flush();
    }
    for(Map.Entry<Channel,Set<Integer>> entry : pending.entrySet()){
      entry.getKey().flush();
      while(entry.getValue().size() > 0 ){
        try {
          PrintUtilities.printAndLog(log,"Waiting " + entry.getKey().remoteAddress() + " " + entry.getValue().size());
          //          PrintUtilities.printList(entry.getValue());
          Thread.sleep(Math.min(Math.max(entry.getValue().size()*100,500),50000));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void acknowledge(Channel owner, int ackMessageId) {
    pending.get(owner).remove(ackMessageId);
  }

  public static Map<String, ChannelFuture> getNodes() {
    return nodes;
  }
}
