package requests;
import java.nio.ByteBuffer;
import shared.CompactArray;
import shared.Cursor;
import shared.RequestTopic;
import shared.TagBuffer;
import shared.serializer.CursorSerializer;
import shared.serializer.ElementSerializer;
import shared.serializer.RequestTopicSerializer;
import util.StreamUtils;
public class DescribeTopicPartitionsRequest
    extends RequestBody<DescribeTopicPartitionsRequest> {
  private CompactArray<RequestTopic> topicsArray;
  private int responsePartitionLimit;
  private Cursor cursor;
  private TagBuffer tagBuffer;
  public CompactArray<RequestTopic> getTopicsArray() { return topicsArray; }
  public int getResponsePartitionLimit() { return responsePartitionLimit; }
  public Cursor getCursor() { return cursor; }
  public TagBuffer getTagBuffer() { return tagBuffer; }
  @Override
  public DescribeTopicPartitionsRequest fromByteBuffer(ByteBuffer data) {
    ElementSerializer<RequestTopic> requestTopicSerializer =
        new RequestTopicSerializer();
    this.topicsArray = CompactArray.fromByteBuffer(data, requestTopicSerializer,
                                                   RequestTopic::new);
    this.responsePartitionLimit = data.getInt();
    this.cursor = Cursor.from(data, new CursorSerializer());
    this.tagBuffer = TagBuffer.fromByteBuffer(data);
    return this;
  }
  @Override
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      CursorSerializer serializer = new CursorSerializer();
      dos.write(this.topicsArray.toBytes());
      dos.writeInt(this.responsePartitionLimit);
      dos.write(serializer.toBytes(this.cursor));
      dos.write(this.tagBuffer.toBytes());
    });
  }
}