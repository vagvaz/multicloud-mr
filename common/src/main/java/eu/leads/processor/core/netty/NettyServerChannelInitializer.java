package eu.leads.processor.core.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

/**
 * Created by vagvaz on 11/25/15.
 */
public class NettyServerChannelInitializer extends ChannelInitializer<SocketChannel> {

  public NettyServerChannelInitializer(){

  }
  @Override protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
//    ch.config();
//    pipeline.addLast(new NettyMessageDecoder());
//    pipeline.addLast(new NettyMessageHandler());
    pipeline.addLast(new ObjectEncoder());
    pipeline.addLast(new ObjectDecoder(1024*1024*50,ClassResolvers.cacheDisabled(null)));
    pipeline.addLast(new NettyMessageHandler());
  }
}
