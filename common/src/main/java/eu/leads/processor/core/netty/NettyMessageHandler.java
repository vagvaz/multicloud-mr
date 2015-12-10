package eu.leads.processor.core.netty;

import eu.leads.processor.common.infinispan.TupleBuffer;
import eu.leads.processor.common.utils.PrintUtilities;
import eu.leads.processor.core.IntermediateDataIndex;
import eu.leads.processor.infinispan.ComplexIntermediateKey;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by vagvaz on 11/25/15.
 */
public class NettyMessageHandler extends ChannelInboundHandlerAdapter {
  private Logger log = LoggerFactory.getLogger(this.getClass());
  ThreadPoolExecutor threadPoolExecutor;
  int received = 0;
  int replied = 0;

  public NettyMessageHandler() {
    threadPoolExecutor = new ThreadPoolExecutor(4,8,1000, TimeUnit.MILLISECONDS,  new LinkedBlockingDeque<Runnable>());
  }

  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//    System.err.println("NEtyyHandler " );
      if(msg instanceof AcknowledgeMessage){
        AcknowledgeMessage ack  = (AcknowledgeMessage) msg;
        NettyDataTransport.acknowledge(ctx.channel(),ack.getAckMessageId());
      }
      else if(msg instanceof  NettyMessage) {
          received++;


          NettyMessage nettyMessage = (NettyMessage) msg;
          NettyMessageRunnable runnable = new NettyMessageRunnable(ctx,nettyMessage);
          threadPoolExecutor.submit(runnable);
      }
      else if (msg instanceof KeyRequest){
        KeyRequest keyRequest = (KeyRequest)msg;
        if(keyRequest.getValue() == null){
          IntermediateDataIndex index = IndexManager.getIndex(keyRequest.getCache());
          if(index != null) {
            Serializable value = index.getKey(keyRequest.getKey());
            keyRequest.setValue(value);
          } else{
            keyRequest.setValue("");
          }
          ctx.writeAndFlush(keyRequest);
        }else{
          System.err.println("key-request-arrived: " + keyRequest.getKey() + "-->" + keyRequest.getValue().toString());
        }
      }
    else{
            PrintUtilities.printAndLog(log,"Unknown message Class " + msg.getClass().getCanonicalName().toString());
      }
//    PrintUtilities.printAndLog(log,"end: received " + received + " replied " + replied);
  }

  @Override public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//    super.channelReadComplete(ctx);
    ctx.flush();
  }

  private void replyForMessage(ChannelHandlerContext ctx, NettyMessage nettyMessage) {
//    ByteBuf buf =  io.netty.buffer.Unpooled.buffer(4);
//    buf.writeInt(nettyMessage.getMessageId());
//    ctx.writeAndFlush(buf);
    AcknowledgeMessage acknowledgeMessage = new AcknowledgeMessage(nettyMessage.getMessageId());
    ctx.writeAndFlush(acknowledgeMessage);
    replied++;
  }

}
