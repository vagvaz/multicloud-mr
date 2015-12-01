package eu.leads.processor.core.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

/**
 * Created by vagvaz on 11/25/15.
 */
public class NettyServerChannelInitializer extends ChannelInitializer<SocketChannel> {

  public NettyServerChannelInitializer(){

  }
  @Override protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    pipeline.addLast(new NettyMessageDecoder());
    pipeline.addLast(new NettyMessageHandler());

  }
}
