package eu.leads.processor.core.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

/**
 * Created by vagvaz on 11/25/15.
 */
public class NettyClientChannelInitializer extends ChannelInitializer<SocketChannel> {

  public NettyClientChannelInitializer(){

  }
  @Override protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    pipeline.addLast(new ObjectEncoder());
    pipeline.addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
    pipeline.addLast(new NettyMessageHandler());
  }
}
