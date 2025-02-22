package log;
import java.nio.ByteBuffer;
public class ValueRecordFactory {
  public static ValueRecord fromByteBuffer(ByteBuffer data) {
    int pos = data.position();
    data.position(pos + 1);
    byte type = data.get();
    data.position(pos);
    ValueRecord record = switch (type) {
            case 12 -> new FeatureLevelRecord();
            case 2 -> new TopicRecord();
            case 3 -> new PartitionRecord();
            default -> throw new IllegalArgumentException("Unknown record type: " + type);
        };
        record.parse(data);
        return record;
    }
}