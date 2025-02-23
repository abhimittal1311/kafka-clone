package responses;
import java.nio.ByteBuffer;
import requests.Request;
public class UnimplementedResponse extends ResponseBody {
  private UnimplementedResponse() {}
  public static UnimplementedResponse fromRequest(Request<?> request) {
    return new UnimplementedResponse();
  }
  @Override
  public ResponseBody fromBytebuffer(ByteBuffer data) {
    return null;
  }
  @Override
  public byte[] toBytes() {
    return new byte[0];
  }
}