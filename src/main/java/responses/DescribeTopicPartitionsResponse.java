package responses;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import log.PartitionRecord;
import log.Record;
import log.RecordBatch;
import log.TopicRecord;
import requests.DescribeTopicPartitionsRequest;
import shared.*;
import shared.serializer.CursorSerializer;
import shared.serializer.PartitionSerializer;
import shared.serializer.ResponseTopicSerializer;
import util.StreamUtils;
public class DescribeTopicPartitionsResponse extends ResponseBody {
  private int throttleTime = 0;
  private CompactArray<ResponseTopic> topicsArray;
  private Cursor nextCursor;
  private TagBuffer tagBuffer = new TagBuffer();
  public DescribeTopicPartitionsResponse(DescribeTopicPartitionsRequest req,
                                         List<RecordBatch> batches) {
    List<ResponseTopic> respTopics = new ArrayList<>();
    for (RequestTopic reqTopic : req.getTopicsArray().getElements()) {
      respTopics.add(responseForRequestTopic(reqTopic, batches));
    }
    this.topicsArray =
        CompactArray.withElements(respTopics, new ResponseTopicSerializer());
    this.nextCursor = Cursor.nullCursor();
  }
  private ResponseTopic responseForRequestTopic(RequestTopic requestTopic,
                                                List<RecordBatch> batches) {
    ResponseTopic responseTopic = new ResponseTopic();
    responseTopic.setTopicName(requestTopic.getTopicName());
    getTopicUUID(requestTopic, batches)
        .ifPresentOrElse(
            topicUUID
            -> {
              responseTopic.setTopicID(topicUUID);
              responseTopic.setPartitionCompactArray(
                  buildPartitionsArrayForTopic(topicUUID, batches));
            },
            () -> {
              System.err.println("Failed to find UUID for topic: " +
                                 requestTopic.getTopicName() +
                                 " in record batches.");
              responseTopic.setErrorCode((short)3);
            });
    return responseTopic;
  }
  private static Optional<UUID> getTopicUUID(RequestTopic requestTopic,
                                             List<RecordBatch> batches) {
    return batches.stream()
        .flatMap(batch -> batch.getRecords().stream())
        .map(Record::getValue)
        .filter(val -> val instanceof TopicRecord)
        .map(val -> (TopicRecord)val)
        .filter(topicRecord
                -> topicRecord.getName().equals(requestTopic.getTopicName()))
        .map(TopicRecord::getTopicUUID)
        .findFirst();
  }
  private CompactArray<Partition>
  buildPartitionsArrayForTopic(UUID topicUUID, List<RecordBatch> batches) {
    List<Partition> partitions =
        batches.stream()
            .flatMap(batch -> batch.getRecords().stream())
            .map(Record::getValue)
            .filter(val -> val instanceof PartitionRecord)
            .map(val -> (PartitionRecord)val)
            .filter(partitionRecord
                    -> partitionRecord.getTopicUUID().equals(topicUUID))
            .map(partitionRecord -> {
              Partition partition = new Partition();
              partition.setLeaderID(partitionRecord.getLeader());
              partition.setPartitionIndex(partitionRecord.getPartitionID());
              return partition;
            })
            .toList();
    return CompactArray.withElements(partitions, new PartitionSerializer());
  }
  @Override
  public DescribeTopicPartitionsResponse fromBytebuffer(ByteBuffer data) {
    this.throttleTime = data.getInt();
    this.topicsArray = CompactArray.fromByteBuffer(
        data, new ResponseTopicSerializer(), ResponseTopic::new);
    this.nextCursor = Cursor.from(data, new CursorSerializer());
    this.tagBuffer = TagBuffer.fromByteBuffer(data);
    return this;
  }
  @Override
  public byte[] toBytes() {
    CursorSerializer serializer = new CursorSerializer();
    return StreamUtils.toBytes(dos -> {
      dos.writeInt(throttleTime);
      dos.write(topicsArray.toBytes());
      dos.write(serializer.toBytes(nextCursor));
      dos.write(tagBuffer.toBytes());
    });
  }
}