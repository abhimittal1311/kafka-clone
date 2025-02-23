package responses;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import requests.FetchRequest;
import requests.Request;
import shared.*;
import shared.serializer.ByteArraySerializer;
import shared.serializer.PartitionResponseSerializer;
import shared.serializer.TopicResponseSerializer;
import util.StreamUtils;
public class FetchResponse extends ResponseBody {
  private final int throttleTimeMs;
  private final short errorCode;
  private final int sessionId;
  private final CompactArray<TopicResponse> responses;
  private final TagBuffer tg;
  private FetchResponse(int throttleTimeMs, short errorCode, int sessionId,
                        CompactArray<TopicResponse> responses, TagBuffer tg) {
    this.throttleTimeMs = throttleTimeMs;
    this.errorCode = errorCode;
    this.sessionId = sessionId;
    this.responses = responses;
    this.tg = tg;
  }
  protected static FetchResponse
  fromRequest(Request<?> req, Map<Path, byte[]> messages,
              Map<UUID, List<String>> topicToMessagePath) {
    FetchRequest fetchReq = (FetchRequest)req.body();
    List<UUID> requestedIds = fetchReq.getTopics()
                                  .getElements()
                                  .stream()
                                  .map(RequestTopicElement::getTopicID)
                                  .toList();
    if (requestedIds.isEmpty()) {
      return new FetchResponse(
          0, (short)0, 0, CompactArray.empty(new TopicResponseSerializer()),
          new TagBuffer());
    }
    List<TopicResponse> topicResponses = new ArrayList<>();
    for (UUID uuid : requestedIds) {
      if (topicToMessagePath.containsKey(uuid)) {
        List<String> dirs = topicToMessagePath.get(uuid);
        if (!dirs.isEmpty()) {
          PartitionResponse partitionResponse =
              PartitionResponse.EmptyTopicPartitionResponse();
          List<byte[]> records = new ArrayList<>();
          for (Path path : messages.keySet()) {
            for (String dir : dirs) {
              if (path.toString().contains(dir)) {
                records.add(messages.get(path));
              }
            }
          }
          partitionResponse.setRecords(
              CompactArray.withElements(records, new ByteArraySerializer()));
          topicResponses.add(new TopicResponse(
              uuid,
              CompactArray.withElements(List.of(partitionResponse),
                                        new PartitionResponseSerializer()),
              new TagBuffer()));
        } else {
          System.err.println("Topic UUID: " + uuid + " has no partitions!");
          topicResponses.add(new TopicResponse(
              uuid, CompactArray.empty(new PartitionResponseSerializer()),
              new TagBuffer()));
        }
      } else {
        // Topic is unknown to us.
        System.err.println("Unknown topic id: " + uuid);
        topicResponses.add(new TopicResponse(
            uuid,
            CompactArray.withElements(
                List.of(PartitionResponse.UnknownTopicPartitionResponse()),
                new PartitionResponseSerializer()),
            new TagBuffer()));
      }
    }
    return new FetchResponse(0, (short)0, 0,
                             CompactArray.withElements(
                                 topicResponses, new TopicResponseSerializer()),
                             new TagBuffer());
  }
  @Override
  public ResponseBody fromBytebuffer(ByteBuffer data) {
    return new FetchResponse(
        data.getInt(), data.getShort(), data.getInt(),
        CompactArray.fromByteBuffer(data, new TopicResponseSerializer()),
        TagBuffer.fromByteBuffer(data));
  }
  @Override
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      dos.writeInt(this.throttleTimeMs);
      dos.writeShort(this.errorCode);
      dos.writeInt(this.sessionId);
      dos.write(this.responses.toBytes());
      dos.write(this.tg.toBytes());
    });
  }
}