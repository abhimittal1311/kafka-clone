package log;
import java.nio.ByteBuffer;
import java.util.UUID;
import shared.CompactString;
import shared.VarInt;
public class TopicRecord extends ValueRecord {
  private CompactString name;
  private UUID topicUUID;
  protected TopicRecord() {}
  public CompactString getName() { return name; }
  public UUID getTopicUUID() { return topicUUID; }
  @Override
  protected void parse(ByteBuffer data) {
    this.frameVersion = data.get();
    this.type = data.get();
    this.version = data.get();
    this.name = CompactString.fromByteBuffer(data);
    this.topicUUID = new UUID(data.getLong(), data.getLong());
    this.taggedFieldsCount = VarInt.fromByteBuffer(data);
  }
}