package requests;
import java.nio.ByteBuffer;
import shared.*;
import shared.serializer.ForgottenTopicSerializer;
import shared.serializer.RequestTopicElementSerializer;
import util.StreamUtils;
public class FetchRequest extends RequestBody<FetchRequest> {
  private int maxWaitMs;
  private int minBytes;
  private int maxBytes;
  private byte isolationLevel;
  private int sessionId;
  private int sessionEpoch;
  private CompactArray<RequestTopicElement> topics;
  private CompactArray<ForgottenTopic> forgottenTopics;
  private CompactString rackId;
  private TagBuffer tg;
  public FetchRequest() {}
  FetchRequest(int maxWaitMs, int minBytes, int maxBytes, byte isolationLevel,
               int sessionId, int sessionEpoch,
               CompactArray<RequestTopicElement> topics,
               CompactArray<ForgottenTopic> forgottenTopics,
               CompactString rackId, TagBuffer tg) {
    this.maxWaitMs = maxWaitMs;
    this.minBytes = minBytes;
    this.maxBytes = maxBytes;
    this.isolationLevel = isolationLevel;
    this.sessionId = sessionId;
    this.sessionEpoch = sessionEpoch;
    this.topics = topics;
    this.forgottenTopics = forgottenTopics;
    this.rackId = rackId;
    this.tg = tg;
  }
  public int getMaxWaitMs() { return maxWaitMs; }
  public int getMinBytes() { return minBytes; }
  public int getMaxBytes() { return maxBytes; }
  public byte getIsolationLevel() { return isolationLevel; }
  public int getSessionId() { return sessionId; }
  public int getSessionEpoch() { return sessionEpoch; }
  public CompactArray<RequestTopicElement> getTopics() { return topics; }
  public CompactArray<ForgottenTopic> getForgottenTopics() {
    return forgottenTopics;
  }
  public CompactString getRackId() { return rackId; }
  @Override
  public FetchRequest fromByteBuffer(ByteBuffer data) {
    return new FetchRequest(
        data.getInt(), data.getInt(), data.getInt(), data.get(), data.getInt(),
        data.getInt(),
        CompactArray.fromByteBuffer(data, new RequestTopicElementSerializer()),
        CompactArray.fromByteBuffer(data, new ForgottenTopicSerializer()),
        CompactString.fromByteBuffer(data), TagBuffer.fromByteBuffer(data));
  }
  @Override
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      dos.writeInt(this.maxWaitMs);
      dos.writeInt(this.minBytes);
      dos.writeInt(this.maxBytes);
      dos.write(this.isolationLevel);
      dos.writeInt(this.sessionId);
      dos.writeInt(this.sessionEpoch);
      dos.write(this.topics.toBytes());
      dos.write(this.forgottenTopics.toBytes());
      dos.write(this.rackId.toBytes());
      dos.write(this.tg.toBytes());
    });
  }
}