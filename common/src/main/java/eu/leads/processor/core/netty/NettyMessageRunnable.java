package eu.leads.processor.core.netty;

import eu.leads.processor.common.infinispan.TupleBuffer;
import eu.leads.processor.common.utils.PrintUtilities;
import io.netty.channel.ChannelHandlerContext;
import org.bson.BSONDecoder;
import org.bson.BasicBSONDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by vagvaz on 12/10/15.
 */
public class NettyMessageRunnable implements Runnable {
  Logger log = LoggerFactory.getLogger(NettyMessageRunnable.class);
  NettyMessage nettyMessage;
  ChannelHandlerContext ctx;
  private int replied = 0;

  public NettyMessageRunnable(ChannelHandlerContext ctx, NettyMessage nettyMessage) {
    this.ctx = ctx;
    this.nettyMessage = nettyMessage;
  }

//  @Override public void run() {
//    try{
//    String indexName = nettyMessage.getCacheName();
//    //        byte[] bytes = Snappy.uncompress( nettyMessage.getBytes());
//    //        ByteArrayInputStream byteArray = new ByteArrayInputStream(bytes);
//    //        ObjectInputStream ois = new ObjectInputStream(byteArray);
//    //        Object firstObject = ois.readObject();
//    //        if(firstObject instanceof TupleBuffer){
//    try {
//      TupleBuffer buffer = new TupleBuffer(nettyMessage.getBytes());//(TupleBuffer)firstObject;
//      for (Map.Entry<Object, Object> entry : buffer.getBuffer().entrySet()) {
//        IndexManager.addToIndex(indexName, entry.getKey(), entry.getValue());
//      }
//    } catch (Exception e) {
//      PrintUtilities.printAndLog(log,
//          "MessageID " + nettyMessage.getMessageId() + " cache " + nettyMessage.getCacheName() + " bytes lenght" + nettyMessage.getBytes().length);
//      PrintUtilities.printAndLog(log, e.getMessage());
//      PrintUtilities.logStackTrace(log, e.getStackTrace());
//    }
//
//
//    //        } else if(firstObject instanceof String || firstObject instanceof ComplexIntermediateKey){
//    //          Object secondObject = ois.readObject();
//    //          IndexManager.addToIndex(indexName,firstObject,secondObject);
//    //        } else{
//    //          PrintUtilities.printAndLog(log,"Unknown class in NettyMessage " + firstObject.getClass().toString());
//    //        }
//  } catch (Exception e) {
//    e.printStackTrace();
//  }
//  replyForMessage(ctx, nettyMessage);
//  }
@Override public void run() {
  try{
    String indexName = nettyMessage.getCacheName();
    //        byte[] bytes = Snappy.uncompress( nettyMessage.getBytes());
    //        ByteArrayInputStream byteArray = new ByteArrayInputStream(bytes);
    //        ObjectInputStream ois = new ObjectInputStream(byteArray);
    //        Object firstObject = ois.readObject();
    //        if(firstObject instanceof TupleBuffer){
    try {
      BSONDecoder decoder = new BasicBSONDecoder();

      byte[] compressed = nettyMessage.getBytes();//bytes;//new byte[compressedSize];
      byte[] uncompressed = Snappy.uncompress(compressed);
      ByteArrayInputStream byteStream = new ByteArrayInputStream(uncompressed);
      ObjectInputStream inputStream = new ObjectInputStream(byteStream);
      try{
        while(true){
          Object key = inputStream.readObject();
          Object tuple = inputStream.readObject();
          IndexManager.addToIndex(indexName,key, tuple);
        }
      }catch (EOFException eof){
        inputStream.close();
        byteStream.close();
      }
    } catch (Exception e) {
      PrintUtilities.printAndLog(log,
          "MessageID " + nettyMessage.getMessageId() + " cache " + nettyMessage.getCacheName() + " bytes lenght" + nettyMessage.getBytes().length);
      PrintUtilities.printAndLog(log, e.getMessage());
      PrintUtilities.logStackTrace(log, e.getStackTrace());
    }
  } catch (Exception e) {
    e.printStackTrace();
  }
  replyForMessage(ctx, nettyMessage);
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
