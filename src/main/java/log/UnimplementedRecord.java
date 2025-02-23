package log;
import java.nio.ByteBuffer;
import shared.VarInt;
import util.HexDump;
import util.StreamUtils;
public class UnimplementedRecord extends ValueRecord {
  private byte[] buf;
  protected UnimplementedRecord() {}
  public byte[] getBuf() { return buf; }
  @Override
  protected void parse(ByteBuffer data) {
    this.length = VarInt.fromByteBuffer(data);
    byte[] buffer = new byte[this.length.getValue()];
    data.get(buffer);
    this.buf = buffer;
    HexDump.printHexDump(this.buf);
  }
  @Override
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      dos.write(this.length.toBytes());
      if (this.length.getValue() > 0) {
        dos.write(this.buf);
      }
    });
  }
}