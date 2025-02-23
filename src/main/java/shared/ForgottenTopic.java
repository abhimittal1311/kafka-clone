package shared;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Supplier;
import shared.serializer.IntegerSerializer;
import util.StreamUtils;
public class ForgottenTopic {
  private UUID topicID;
  private CompactArray<Integer> partitions;
  private TagBuffer tg = new TagBuffer();
  public ForgottenTopic() {}
  public ForgottenTopic(UUID topicID, CompactArray<Integer> partitions,
                        TagBuffer tg) {
    this.topicID = topicID;
    this.partitions = partitions;
    this.tg = tg;
  }
  public UUID getTopicID() { return topicID; }
  public CompactArray<Integer> getPartitions() { return partitions; }
  public TagBuffer getTg() { return tg; }
  public static ForgottenTopic fromByteBuffer(ByteBuffer data) {
    Supplier<Integer> defaultIntValue = () -> - 1;
    UUID topicID = new UUID(data.getLong(), data.getLong());
    return new ForgottenTopic(
        topicID, CompactArray.fromByteBuffer(data, new IntegerSerializer()),
        TagBuffer.fromByteBuffer(data));
  }
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      dos.writeLong(topicID.getMostSignificantBits());
      dos.writeLong(topicID.getLeastSignificantBits());
      dos.write(partitions.toBytes());
      dos.write(tg.toBytes());
    });
  }
}