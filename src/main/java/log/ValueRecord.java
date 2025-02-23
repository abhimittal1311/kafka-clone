package log;
import java.nio.ByteBuffer;
import shared.VarInt;
public abstract class ValueRecord {
  protected VarInt length = new VarInt(-1);
  protected byte frameVersion;
  protected byte type;
  protected byte version;
  protected VarInt taggedFieldsCount;
  public VarInt getLength() { return length; }
  public byte getFrameVersion() { return frameVersion; }
  public byte getType() { return type; }
  public byte getVersion() { return version; }
  public VarInt getTaggedFieldsCount() { return taggedFieldsCount; }
  protected abstract void parse(ByteBuffer data);
  public abstract byte[] toBytes();
}