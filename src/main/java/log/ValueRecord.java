package log;
import java.nio.ByteBuffer;
import shared.VarInt;
public abstract class ValueRecord {
  protected byte frameVersion;
  protected byte type;
  protected byte version;
  protected VarInt taggedFieldsCount;
  public byte getFrameVersion() { return frameVersion; }
  public byte getType() { return type; }
  public byte getVersion() { return version; }
  public VarInt getTaggedFieldsCount() { return taggedFieldsCount; }
  protected abstract void parse(ByteBuffer data);
}