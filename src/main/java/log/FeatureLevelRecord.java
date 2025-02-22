package log;
import java.nio.ByteBuffer;
import shared.CompactString;
import shared.VarInt;
public class FeatureLevelRecord extends ValueRecord {
  private CompactString name;
  private short featureLevel;
  protected FeatureLevelRecord() {}
  public CompactString getName() { return name; }
  public short getFeatureLevel() { return featureLevel; }
  @Override
  public void parse(ByteBuffer data) {
    this.frameVersion = data.get();
    this.type = data.get();
    this.version = data.get();
    this.name = CompactString.fromByteBuffer(data);
    this.featureLevel = data.getShort();
    this.taggedFieldsCount = VarInt.fromByteBuffer(data);
  }
}