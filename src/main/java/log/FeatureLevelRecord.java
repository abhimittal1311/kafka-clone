package log;
import java.nio.ByteBuffer;
import shared.CompactString;
import shared.VarInt;
import util.StreamUtils;
public class FeatureLevelRecord extends ValueRecord {
  private CompactString name;
  private short featureLevel;
  protected FeatureLevelRecord() {}
  public CompactString getName() { return name; }
  public short getFeatureLevel() { return featureLevel; }
  @Override
  public void parse(ByteBuffer data) {
    this.length = VarInt.fromByteBuffer(data);
    this.frameVersion = data.get();
    this.type = data.get();
    this.version = data.get();
    this.name = CompactString.fromByteBuffer(data);
    this.featureLevel = data.getShort();
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
      dos.writeShort(this.featureLevel);
      dos.write(this.taggedFieldsCount.toBytes());
    });
  }
}