package eu.leads.processor.core.netty;

import io.netty.buffer.ByteBuf;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created by vagvaz on 11/25/15.
 */
public class NettyMessage implements Serializable {
  private int messageId;
  String cacheName;
  byte[] bytes;
  public NettyMessage(String cacheName, byte[] bytes, int counter) {
    this.cacheName = cacheName;
    this.bytes = bytes;
    this.messageId = counter;
  }

  public NettyMessage(byte[] decoded) {
    fromByteArray(decoded);
  }

  public String getCacheName() {
    return cacheName;
  }

  public void setCacheName(String cacheName) {
    this.cacheName = cacheName;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public int getMessageId() {
    return messageId;
  }

  public byte[] toByteArray() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeUTF(cacheName);
      oos.writeInt(messageId);
      oos.writeInt(bytes.length);
      oos.write(bytes);
      oos.flush();
      bos.flush();
      oos.close();
      bos.close();
      return bos.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new byte[0];
  }

  public void fromByteArray(byte[] bytes ){
    if(bytes.length <= 1){
      System.err.println("Empty netty message");
      messageId = -1;
      cacheName = "";
      bytes = null;
    }
    ByteArrayInputStream bos = new ByteArrayInputStream(bytes);
    try {
      ObjectInputStream oos = new ObjectInputStream(bos);
      cacheName = oos.readUTF();
      messageId = oos.readInt();
      int size = oos.readInt();
      this.bytes = new byte[size];
      oos.read(this.bytes);
      oos.close();
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
