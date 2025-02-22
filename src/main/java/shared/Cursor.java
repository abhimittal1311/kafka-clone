package shared;
import java.nio.ByteBuffer;
import java.util.Optional;
import shared.serializer.ElementSerializer;
public class Cursor {
  private final CompactString topicName;
  private final Integer partitionIndex;
  public Cursor(CompactString topicName, Integer partitionIndex) {
    this.topicName = topicName;
    this.partitionIndex = partitionIndex;
  }
  public static Cursor nullCursor() { return new Cursor(null, null); }
  public static Cursor from(ByteBuffer data,
                            ElementSerializer<Cursor> serializer) {
    return serializer.fromByteBuffer(data);
  }
  public boolean isNull() {
    return topicName == null && partitionIndex == null;
  }
  public Optional<CompactString> getTopicName() {
    return Optional.of(topicName);
  }
  public Optional<Integer> getPartitionIndex() {
    return Optional.of(partitionIndex);
  }
}