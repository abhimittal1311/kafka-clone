package log;
import java.nio.ByteBuffer;
import shared.VarInt;
public class Record {
  private VarInt length;
  private byte attributes;
  private VarInt timestampDelta;
  private VarInt offsetDelta;
  private Key key;
  private ValueRecord value;
  // unsigned
  private VarInt headersArrayCount;
  public Record() {}
  private Record(VarInt length, byte attributes, VarInt timestampDelta,
                 VarInt offsetDelta, Key key, ValueRecord value,
                 VarInt headersArrayCount) {
    this.length = length;
    this.attributes = attributes;
    this.timestampDelta = timestampDelta;
    this.offsetDelta = offsetDelta;
    this.key = key;
    this.value = value;
    this.headersArrayCount = headersArrayCount;
  }
  public VarInt getLength() { return length; }
  public byte getAttributes() { return attributes; }
  public VarInt getTimestampDelta() { return timestampDelta; }
  public VarInt getOffsetDelta() { return offsetDelta; }
  public Key getKey() { return key; }
  public ValueRecord getValue() { return value; }
  public VarInt getHeadersArrayCount() { return headersArrayCount; }
  public static Record fromByteBuffer(ByteBuffer data) {
    return new Record(
        VarInt.fromByteBuffer(data), data.get(), VarInt.fromByteBuffer(data),
        VarInt.fromByteBuffer(data), Key.fromByteBuffer(data),
        ValueRecordFactory.fromByteBuffer(data), VarInt.fromByteBuffer(data));
  }
}