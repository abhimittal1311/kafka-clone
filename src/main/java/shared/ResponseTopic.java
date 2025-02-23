package shared;
import java.util.UUID;
import shared.serializer.PartitionSerializer;
public class ResponseTopic {
  private short errorCode;
  private CompactString topicName;
  private UUID topicID =
      UUID.fromString("00000000-0000-0000-0000-000000000000");
  private boolean isInternal;
  private CompactArray<Partition> partitionCompactArray =
      CompactArray.empty(new PartitionSerializer());
  private int topicAuthorizedOperations;
  private TagBuffer tagBuffer = new TagBuffer();
  public ResponseTopic() {}
  public ResponseTopic(short errorCode, CompactString topicName, UUID topicID,
                       boolean isInternal,
                       CompactArray<Partition> partitionCompactArray,
                       int topicAuthorizedOperations, TagBuffer tagBuffer) {
    this.errorCode = errorCode;
    this.topicName = topicName;
    this.topicID = topicID;
    this.isInternal = isInternal;
    this.partitionCompactArray = partitionCompactArray;
    this.topicAuthorizedOperations = topicAuthorizedOperations;
    this.tagBuffer = tagBuffer;
  }
  public short getErrorCode() { return errorCode; }
  public CompactString getTopicName() { return topicName; }
  public UUID getTopicID() { return topicID; }
  public boolean isInternal() { return isInternal; }
  public CompactArray<Partition> getPartitionCompactArray() {
    return partitionCompactArray;
  }
  public int getTopicAuthorizedOperations() {
    return topicAuthorizedOperations;
  }
  public TagBuffer getTagBuffer() { return tagBuffer; }
  public void setErrorCode(short errorCode) { this.errorCode = errorCode; }
  public void setTopicName(CompactString topicName) {
    this.topicName = topicName;
  }
  public void setTopicID(UUID topicID) { this.topicID = topicID; }
  public void setInternal(boolean internal) { isInternal = internal; }
  public void
  setPartitionCompactArray(CompactArray<Partition> partitionCompactArray) {
    this.partitionCompactArray = partitionCompactArray;
  }
  public void setTopicAuthorizedOperations(int topicAuthorizedOperations) {
    this.topicAuthorizedOperations = topicAuthorizedOperations;
  }
  public void setTagBuffer(TagBuffer tagBuffer) { this.tagBuffer = tagBuffer; }
}