package requests;
import java.nio.ByteBuffer;
public abstract class RequestBody<T> {
  public abstract T fromByteBuffer(ByteBuffer data);
  public abstract byte[] toBytes();
}