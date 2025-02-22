package shared;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import util.StreamUtils;
public class CompactString {
  private final String value;
  public CompactString(String str) { this.value = str; }
  @Override
  public String toString() {
    return this.value;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CompactString that = (CompactString)obj;
    return Objects.equals(this.value, that.value);
  }
  @Override
  public int hashCode() {
    return Objects.hash(this.value);
  }
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
      dos.write(new VarInt(utf8.length + 1).toBytes());
      dos.write(utf8);
    });
  }
  public static CompactString fromByteBuffer(ByteBuffer data) {
    VarInt len = VarInt.fromByteBuffer(data);
    int strLen = len.getUnsignedValue() - 1;
    byte[] temp = new byte[strLen];
    data.get(temp);
    return new CompactString(new String(temp, StandardCharsets.UTF_8));
  }
}
