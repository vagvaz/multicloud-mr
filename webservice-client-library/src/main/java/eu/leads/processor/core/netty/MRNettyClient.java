package eu.leads.processor.core.netty;

import eu.leads.processor.common.infinispan.ClusterInfinispanManager;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.*;

/**
 * Created by vagvaz on 12/9/15.
 */
public class MRNettyClient {
  JsonObject globalConfiguration;
   EventLoopGroup workerGroup;
   Bootstrap clientBootstrap;
   Set<ChannelFuture> channelFutures;
   Map<String,ChannelFuture> nodes;
    List<String> nodeIds;
   String me;
  Map<Channel,Set<Integer>> pending;
  private  NettyClientChannelInitializer clientChannelInitializer;
  private  ChannelFuture serverFuture;
  private  int counter = 0;
  private  Map<String,Long> histogram;
  private  Logger log = LoggerFactory.getLogger(MRNettyClient.class);
  public MRNettyClient(JsonObject globalConfig) {
    this.globalConfiguration = globalConfig;
    clientChannelInitializer = new NettyClientChannelInitializer();
    pending = new HashMap<>();
    nodes = new TreeMap<>();
    channelFutures = new HashSet<>();
    histogram = new HashMap<>();

    clientBootstrap = new Bootstrap();
    workerGroup = new NioEventLoopGroup();
    clientBootstrap.group(workerGroup);
    clientBootstrap.channel(NioSocketChannel.class);
    clientBootstrap.option(ChannelOption.SO_KEEPALIVE,true).handler(clientChannelInitializer);


    JsonObject componentAddrs = globalConfiguration.getObject("componentsAddrs");
    List<String> clouds = new ArrayList(new TreeSet(componentAddrs.getFieldNames()));
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
            pending.put(f.channel(),new HashSet<Integer>(100));
            channelFutures.add(f);
            histogram.put(host,0L);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
    nodeIds = new ArrayList<>(((TreeMap)nodes).descendingKeySet());
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
  public void request(String word, KeyRequest keyRequest) {
    int index = Math.abs(word.hashCode() ) % nodes.size();
    String target = nodeIds.get(index);
    nodes.get(target).channel().writeAndFlush(keyRequest);
  }

  public void close(){
    for(Map.Entry<String,ChannelFuture> entry : nodes.entrySet()){
      try {
        entry.getValue().sync();
        entry.getValue().channel().close();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
