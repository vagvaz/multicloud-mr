package eu.leads.processor.core.netty;

import eu.leads.processor.common.utils.PrintUtilities;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by vagvaz on 11/26/15.
 */
public class NettyMessageEncoder extends MessageToByteEncoder<NettyMessage> {
  Logger log = LoggerFactory.getLogger(NettyMessageEncoder.class);
  @Override protected void encode(ChannelHandlerContext ctx, NettyMessage msg, ByteBuf out) throws Exception {
//    System.err.println("NettyMESGENC " );

    if(msg.getMessageId() < 5) {
      PrintUtilities.printAndLog(log,
          "MessageID " + msg.getMessageId() + " cache " + msg.getCacheName() + " bytes lenght" + msg.getBytes().length);
    }
    byte[] data  = new byte[100000];//= msg.toByteArray();

    if(data != null){
      for(int i =0; i < data.length;i++){
        data[i] = (byte) (i % 128);
      }
    }

    //
    int dataLenght = data.length;
    out.writeByte((byte) 'F');
    out.writeInt(dataLenght);
    out.writeBytes(data);

  }
}
