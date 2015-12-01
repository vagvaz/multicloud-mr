package eu.leads.processor.core.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.util.List;

/**
 * Created by vagvaz on 11/26/15.
 */
public class NettyMessageDecoder  extends ByteToMessageDecoder {
  @Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    // Wait until the length prefix is available.
    if (in.readableBytes() < 5) {
      return;
    }

    in.markReaderIndex();

    // Check the magic number.
    int magicNumber = in.readUnsignedByte();
    if (magicNumber != 'F') {
      in.resetReaderIndex();
      throw new CorruptedFrameException("Invalid magic number: " + magicNumber);
    }

    // Wait until the whole data is available.
    int dataLength = in.readInt();
    if (in.readableBytes() < dataLength) {
      in.resetReaderIndex();
      return;
    }
    // Convert the received data into a new BigInteger.
    byte[] decoded = new byte[dataLength];
    in.readBytes(decoded);
    out.add(new NettyMessage(decoded));
  }
}
