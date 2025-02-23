package responses;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import log.RecordBatch;
import requests.Request;
import shared.TagBuffer;
import util.StreamUtils;
public class Response {
  private ResponseHeader responseHeader;
  private ResponseBody body;
  public Response(ResponseHeader responseHeader) {
    this.responseHeader = responseHeader;
  }
  public Response(Request<?> request, List<RecordBatch> batches,
                  Map<Path, byte[]> messages,
                  Map<UUID, List<String>> topicToMessagePath) {
    switch (request.header().getRequestAPIKey()) {
            case 1 -> {
                this.responseHeader = new ResponseHeader(request.header().getCorrelationId(), new TagBuffer());
                this.body = FetchResponse.fromRequest(request, messages, topicToMessagePath);
            }
            case 18 -> {
                this.responseHeader = new ResponseHeader(request.header().getCorrelationId(), null);
                this.body = APIVersionsResponse.fromRequest(request);
            }
            case 75 -> {
                this.responseHeader = new ResponseHeader(request.header().getCorrelationId(), new TagBuffer());
                this.body = DescribeTopicPartitionsResponse.fromRequest(request, batches);
            }
            default -> {
                this.responseHeader = new ResponseHeader(request.header().getCorrelationId(), new TagBuffer());
                this.body = UnimplementedResponse.fromRequest(request);
            }
        }
        ;
    }
    public ResponseHeader getHeader() {
        return responseHeader;
    }
    public void setHeader(ResponseHeader responseHeader) {
        this.responseHeader = responseHeader;
    }
    public ResponseBody getBody() {
        return body;
    }
    public byte[] toBytes() {
        return StreamUtils.toBytes(dos -> {
            byte[] headerBytes = responseHeader.toBytes();
            byte[] bodyBytes = body.toBytes();
            dos.writeInt(headerBytes.length + bodyBytes.length);
            dos.write(headerBytes);
            dos.write(bodyBytes);
        });
    }
    public static Response fromByteBuffer(ByteBuffer data) {
        int messageSize = data.getInt();
        int start = data.position();
        ResponseHeader responseHeader = ResponseHeader.fromByteBuffer(data);
        int headerSize = data.position() - start;
        byte[] body = new byte[messageSize - headerSize];
        data.get(body);
        return new Response(responseHeader);
    }
}