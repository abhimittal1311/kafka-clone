package responses;
import java.nio.ByteBuffer;
public abstract class ResponseBody {
  public abstract ResponseBody fromBytebuffer(ByteBuffer data);
  public abstract byte[] toBytes();
}