package eu.leads.processor.core.netty;

import eu.leads.processor.common.infinispan.TupleBuffer;
import eu.leads.processor.common.utils.PrintUtilities;
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
import java.util.Map;

/**
 * Created by vagvaz on 11/25/15.
 */
public class NettyMessageHandler extends ChannelInboundHandlerAdapter {
  private Logger log = LoggerFactory.getLogger(this.getClass());
  int received = 0;
  int replied = 0;
  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      try{
        received++;
         NettyMessage nettyMessage = (NettyMessage)msg;
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
        }catch(Exception e){
          PrintUtilities.printAndLog(log,"MessageID " + nettyMessage.getMessageId() + " cache " + nettyMessage.getCacheName() + " bytes lenght" + nettyMessage.getBytes().length);
          PrintUtilities.printAndLog(log,e.getMessage());
          PrintUtilities.logStackTrace(log,e.getStackTrace());
        }


        //        } else if(firstObject instanceof String || firstObject instanceof ComplexIntermediateKey){
//          Object secondObject = ois.readObject();
//          IndexManager.addToIndex(indexName,firstObject,secondObject);
//        } else{
//          PrintUtilities.printAndLog(log,"Unknown class in NettyMessage " + firstObject.getClass().toString());
//        }
      } catch (Exception e){
        e.printStackTrace();
      }
    replyForMessage(ctx, (NettyMessage) msg);

    PrintUtilities.printAndLog(log,"end: received " + received + " replied " + replied);
  }

  private void replyForMessage(ChannelHandlerContext ctx, NettyMessage nettyMessage) {
    ByteBuf buf =  io.netty.buffer.Unpooled.buffer(8);
    buf.writeLong(nettyMessage.getMessageId());
    ctx.writeAndFlush(buf);
    replied++;
  }

}
