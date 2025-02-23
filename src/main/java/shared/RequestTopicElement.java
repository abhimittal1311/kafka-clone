package shared;
import java.nio.ByteBuffer;
import java.util.UUID;
import shared.serializer.RequestPartitionSerializer;
import util.StreamUtils;
public class RequestTopicElement {
  private UUID topicID;
  private CompactArray<RequestPartition> partitions;
  private TagBuffer tg;
  public RequestTopicElement() {}
  public RequestTopicElement(UUID topicID,
                             CompactArray<RequestPartition> partitions,
                             TagBuffer tg) {
    this.topicID = topicID;
    this.partitions = partitions;
    this.tg = tg;
  }
  public UUID getTopicID() { return topicID; }
  public CompactArray<RequestPartition> getPartitions() { return partitions; }
  public TagBuffer getTg() { return tg; }
  public static RequestTopicElement fromByteBuffer(ByteBuffer data) {
    UUID topicID = new UUID(data.getLong(), data.getLong());
    return new RequestTopicElement(
        topicID,
        CompactArray.fromByteBuffer(data, new RequestPartitionSerializer()),
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