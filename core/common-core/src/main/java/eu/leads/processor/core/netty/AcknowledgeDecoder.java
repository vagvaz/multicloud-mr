package eu.leads.processor.core.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * Created by vagvaz on 11/25/15.
 */
public class AcknowledgeDecoder extends ByteToMessageDecoder {
  @Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    in.markReaderIndex();
    if(in.readableBytes() < 4){
      in.resetReaderIndex();
      return;
    }
    AcknowledgeMessage message = new AcknowledgeMessage(in.readInt());
    out.add(message);
  }
}
