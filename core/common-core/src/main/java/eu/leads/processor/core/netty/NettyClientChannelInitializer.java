package eu.leads.processor.core.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * Created by vagvaz on 11/25/15.
 */
public class NettyClientChannelInitializer extends ChannelInitializer<SocketChannel> {

  public NettyClientChannelInitializer(){

  }
  @Override protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast(new NettyMessageEncoder());
    pipeline.addLast(new AcknowledgeDecoder());
    pipeline.addLast("ackMessageHandler", new AcknowledgeHandler(ch));
  }
}
