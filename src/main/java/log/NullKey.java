package log;
public class NullKey extends Key {
  @Override
  public byte[] toBytes() {
    return new byte[] {-1};
  }
}