package log;
import java.nio.ByteBuffer;
import java.util.UUID;
import shared.CompactString;
import shared.VarInt;
import util.StreamUtils;
public class TopicRecord extends ValueRecord {
  private CompactString name;
  private UUID topicUUID;
  protected TopicRecord() {}
  public CompactString getName() { return name; }
  public UUID getTopicUUID() { return topicUUID; }
  @Override
  protected void parse(ByteBuffer data) {
    this.length = VarInt.fromByteBuffer(data);
    this.frameVersion = data.get();
    this.type = data.get();
    this.version = data.get();
    this.name = CompactString.fromByteBuffer(data);
    this.topicUUID = new UUID(data.getLong(), data.getLong());
    this.taggedFieldsCount = VarInt.fromByteBuffer(data);
  }
  @Override
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      dos.write(this.length.toBytes());
      dos.write(this.frameVersion);
      dos.write(this.type);
      dos.write(this.version);
      dos.write(this.name.toBytes());
      dos.writeLong(this.topicUUID.getMostSignificantBits());
      dos.writeLong(this.topicUUID.getLeastSignificantBits());
      dos.write(this.taggedFieldsCount.toBytes());
    });
  }
}