package log;
import java.nio.ByteBuffer;
import shared.VarInt;
public class ValueRecordFactory {
  public static ValueRecord fromByteBuffer(ByteBuffer data) {
    int pos = data.position();
    // Skip some bytes to get the type, then rewind.
    VarInt.fromByteBuffer(data);
    data.get();
    byte type = data.get();
    data.position(pos);
    ValueRecord record = switch (type) {
            case 12 -> new FeatureLevelRecord();
            case 2 -> new TopicRecord();
            case 3 -> new PartitionRecord();
            default -> {
                System.err.println("Unknown record type: " + type);
                yield new UnimplementedRecord();
            }
        };
        record.parse(data);
        return record;
    }
}