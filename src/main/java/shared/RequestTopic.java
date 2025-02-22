package shared;
public class RequestTopic {
  private CompactString topicName;
  private TagBuffer topicTagBuffer;
  public RequestTopic() {}
  public RequestTopic(CompactString topicName, TagBuffer topicTagBuffer) {
    this.topicName = topicName;
    this.topicTagBuffer = topicTagBuffer;
  }
  public CompactString getTopicName() { return topicName; }
  public TagBuffer getTopicTagBuffer() { return topicTagBuffer; }
}