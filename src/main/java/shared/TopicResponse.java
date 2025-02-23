package shared;
import java.nio.ByteBuffer;
import java.util.UUID;
import shared.serializer.PartitionResponseSerializer;
import util.StreamUtils;
public class TopicResponse {
  private UUID topicID;
  private CompactArray<PartitionResponse> partitions;
  private TagBuffer tg = new TagBuffer();
  public TopicResponse() {}
  public TopicResponse(UUID topicID, CompactArray<PartitionResponse> partitions,
                       TagBuffer tg) {
    this.topicID = topicID;
    this.partitions = partitions;
    this.tg = tg;
  }
  public UUID getTopicID() { return topicID; }
  public void setTopicID(UUID topicID) { this.topicID = topicID; }
  public CompactArray<PartitionResponse> getPartitions() { return partitions; }
  public void setPartitions(CompactArray<PartitionResponse> partitions) {
    this.partitions = partitions;
  }
  public TagBuffer getTg() { return tg; }
  public void setTg(TagBuffer tg) { this.tg = tg; }
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      dos.writeLong(this.topicID.getMostSignificantBits());
      dos.writeLong(this.topicID.getLeastSignificantBits());
      dos.write(this.partitions.toBytes());
      dos.write(this.tg.toBytes());
    });
  }
  public static TopicResponse fromByteBuffer(ByteBuffer data) {
    return new TopicResponse(
        new UUID(data.getLong(), data.getLong()),
        CompactArray.fromByteBuffer(data, new PartitionResponseSerializer()),
        TagBuffer.fromByteBuffer(data));
  }
}