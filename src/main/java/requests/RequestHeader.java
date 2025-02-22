package requests;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import shared.TagBuffer;
import util.StreamUtils;
public class RequestHeader {
  private final short requestAPIKey;
  private final short requestAPIVersion;
  private final int correlationId;
  private final String clientId;
  private final TagBuffer tagBuffer;
  public RequestHeader(short requestAPIKey, short requestAPIVersion,
                       int correlationId, String clientId,
                       TagBuffer tagBuffer) {
    this.requestAPIKey = requestAPIKey;
    this.requestAPIVersion = requestAPIVersion;
    this.correlationId = correlationId;
    this.clientId = clientId;
    this.tagBuffer = tagBuffer;
  }
  public short getRequestAPIKey() { return requestAPIKey; }
  public short getRequestAPIVersion() { return requestAPIVersion; }
  public int getCorrelationId() { return correlationId; }
  public String getClientId() { return clientId; }
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      dos.writeShort(requestAPIKey);
      dos.writeShort(requestAPIVersion);
      dos.writeInt(correlationId);
      if (!clientId.isEmpty()) {
        dos.writeShort(clientId.length());
        dos.write(clientId.getBytes(StandardCharsets.UTF_8));
      } else {
        dos.writeShort(-1);
      }
      dos.write(tagBuffer.toBytes());
    });
  }
  public static RequestHeader fromByteBuffer(ByteBuffer data) {
    short requestAPIKey = data.getShort();
    short requestAPIVersion = data.getShort();
    int correlationId = data.getInt();
    short clientIdLen = data.getShort();
    String clientId = "";
    if (clientIdLen != -1) {
      byte[] buf = new byte[clientIdLen];
      data.get(buf);
      clientId = new String(buf, StandardCharsets.UTF_8);
    }
    TagBuffer tagBuffer = TagBuffer.fromByteBuffer(data);
    return new RequestHeader(requestAPIKey, requestAPIVersion, correlationId,
                             clientId, tagBuffer);
  }
}