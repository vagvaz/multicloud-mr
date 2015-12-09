package eu.leads.processor.core.netty;

import eu.leads.processor.common.utils.PrintUtilities;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by vagvaz on 11/25/15.
 */
public class AcknowledgeHandler extends ChannelInboundHandlerAdapter {
  Channel owner;
  int ack = 0;
  Logger log = LoggerFactory.getLogger(AcknowledgeHandler.class);
  public AcknowledgeHandler(SocketChannel ch) {
    this.owner = ch;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ack++;
//    System.err.println("AckHandler " );
//    PrintUtilities.printAndLog(log,"Acked: " + ack);
    AcknowledgeMessage buf = (AcknowledgeMessage) msg;
    NettyDataTransport.acknowledge(owner,buf.getAckMessageId());
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
//    System.err.println("AckREADComplet " );
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    // Close the connection when an exception is raised.
    cause.printStackTrace();
    ctx.close();
  }
}
