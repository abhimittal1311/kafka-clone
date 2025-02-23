package log;
import java.nio.ByteBuffer;
import shared.VarInt;
import util.StreamUtils;
public class Key {
  private final byte[] key;
  private Key(byte[] key) { this.key = key; }
  protected Key() { this.key = null; }
  public static Key fromByteBuffer(ByteBuffer data) {
    int len = VarInt.fromByteBuffer(data).getValue();
    if (len == -1) {
      return new NullKey();
    }
    byte[] bytes = new byte[len];
    data.get(bytes);
    return new Key(bytes);
  }
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      if (this.key != null) {
        dos.write(new VarInt(this.key.length).toBytes());
        dos.write(this.key);
      }
    });
  }
}
