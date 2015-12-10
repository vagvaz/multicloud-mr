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

/**
 * Created by vagvaz on 11/25/15.
 */
public class NettyMessageHandler extends ChannelInboundHandlerAdapter {
  private Logger log = LoggerFactory.getLogger(this.getClass());
  int received = 0;
  int replied = 0;
  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//    System.err.println("NEtyyHandler " );
      if(msg instanceof AcknowledgeMessage){
        AcknowledgeMessage ack  = (AcknowledgeMessage) msg;
        NettyDataTransport.acknowledge(ctx.channel(),ack.getAckMessageId());
      }
      else if(msg instanceof  NettyMessage) {
        try {
          received++;
          NettyMessage nettyMessage = (NettyMessage) msg;
          String indexName = nettyMessage.getCacheName();
          //        byte[] bytes = Snappy.uncompress( nettyMessage.getBytes());
          //        ByteArrayInputStream byteArray = new ByteArrayInputStream(bytes);
          //        ObjectInputStream ois = new ObjectInputStream(byteArray);
          //        Object firstObject = ois.readObject();
          //        if(firstObject instanceof TupleBuffer){
          try {
            TupleBuffer buffer = new TupleBuffer(nettyMessage.getBytes());//(TupleBuffer)firstObject;
            for (Map.Entry<Object, Object> entry : buffer.getBuffer().entrySet()) {
              IndexManager.addToIndex(indexName, entry.getKey(), entry.getValue());
            }
          } catch (Exception e) {
            PrintUtilities.printAndLog(log,
                "MessageID " + nettyMessage.getMessageId() + " cache " + nettyMessage.getCacheName() + " bytes lenght" + nettyMessage.getBytes().length);
            PrintUtilities.printAndLog(log, e.getMessage());
            PrintUtilities.logStackTrace(log, e.getStackTrace());
          }


          //        } else if(firstObject instanceof String || firstObject instanceof ComplexIntermediateKey){
          //          Object secondObject = ois.readObject();
          //          IndexManager.addToIndex(indexName,firstObject,secondObject);
          //        } else{
          //          PrintUtilities.printAndLog(log,"Unknown class in NettyMessage " + firstObject.getClass().toString());
          //        }
        } catch (Exception e) {
          e.printStackTrace();
        }
        replyForMessage(ctx, (NettyMessage) msg);
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
