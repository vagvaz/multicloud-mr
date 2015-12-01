package eu.leads.processor.core.netty;

/**
 * Created by vagvaz on 11/25/15.
 */
public class AcknowledgeMessage {
  private int ackMessageId;

  public AcknowledgeMessage(int l) {
    this.ackMessageId = l;
  }

  public int getAckMessageId() {
    return ackMessageId;
  }

  public void setAckMessageId(int ackMessageId) {
    this.ackMessageId = ackMessageId;
  }
  public AcknowledgeMessage(NettyMessage nettyMessage) {
    ackMessageId = nettyMessage.getMessageId();
  }
}
