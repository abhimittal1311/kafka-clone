package responses;
import java.nio.ByteBuffer;
import java.util.List;
import log.RecordBatch;
import requests.APIVersionsRequest;
import requests.DescribeTopicPartitionsRequest;
import requests.Request;
import shared.TagBuffer;
import util.StreamUtils;
public class Response {
  private ResponseHeader responseHeader;
  private ResponseBody body;
  public Response(ResponseHeader responseHeader) {
    this.responseHeader = responseHeader;
  }
  public Response(Request<?> request, List<RecordBatch> batches) {
    this.responseHeader =
        new ResponseHeader(request.header().getCorrelationId());
    if (request.body() instanceof APIVersionsRequest) {
      this.body = new APIVersionsResponse(request);
    } else if (request.body() instanceof DescribeTopicPartitionsRequest) {
      this.responseHeader = new ResponseHeader(
          request.header().getCorrelationId(), new TagBuffer());
      this.body = new DescribeTopicPartitionsResponse(
          (DescribeTopicPartitionsRequest)request.body(), batches);
    }
  }
  public ResponseHeader getHeader() { return responseHeader; }
  public void setHeader(ResponseHeader responseHeader) {
    this.responseHeader = responseHeader;
  }
  public ResponseBody getBody() { return body; }
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