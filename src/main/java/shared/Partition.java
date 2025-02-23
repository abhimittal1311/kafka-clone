package shared;
import shared.serializer.ReplicaNodeSerializer;
public class Partition {
  private short errorCode;
  private int partitionIndex;
  private int leaderID;
  private int leaderEpoch;
  private CompactArray<ReplicaNode> replicaNodeArray =
      CompactArray.empty(new ReplicaNodeSerializer());
  private CompactArray<ReplicaNode> isrNodeArray =
      CompactArray.empty(new ReplicaNodeSerializer());
  private CompactArray<ReplicaNode> eligibleLeaderReplicas =
      CompactArray.empty(new ReplicaNodeSerializer());
  private CompactArray<ReplicaNode> lastKnownELR =
      CompactArray.empty(new ReplicaNodeSerializer());
  private CompactArray<ReplicaNode> offlineReplicas =
      CompactArray.empty(new ReplicaNodeSerializer());
  private TagBuffer tagBuffer = new TagBuffer();
  public Partition() {}
  public Partition(short errorCode, int partitionIndex, int leaderID,
                   int leaderEpoch, CompactArray<ReplicaNode> replicaNodeArray,
                   CompactArray<ReplicaNode> isrNodeArray,
                   CompactArray<ReplicaNode> eligibleLeaderReplicas,
                   CompactArray<ReplicaNode> lastKnownELR,
                   CompactArray<ReplicaNode> offlineReplicas,
                   TagBuffer tagBuffer) {
    this.errorCode = errorCode;
    this.partitionIndex = partitionIndex;
    this.leaderID = leaderID;
    this.leaderEpoch = leaderEpoch;
    this.replicaNodeArray = replicaNodeArray;
    this.isrNodeArray = isrNodeArray;
    this.eligibleLeaderReplicas = eligibleLeaderReplicas;
    this.lastKnownELR = lastKnownELR;
    this.offlineReplicas = offlineReplicas;
    this.tagBuffer = tagBuffer;
  }
  public short getErrorCode() { return errorCode; }
  public int getPartitionIndex() { return partitionIndex; }
  public int getLeaderID() { return leaderID; }
  public int getLeaderEpoch() { return leaderEpoch; }
  public CompactArray<ReplicaNode> getReplicaNodeArray() {
    return replicaNodeArray;
  }
  public CompactArray<ReplicaNode> getIsrNodeArray() { return isrNodeArray; }
  public CompactArray<ReplicaNode> getEligibleLeaderReplicas() {
    return eligibleLeaderReplicas;
  }
  public CompactArray<ReplicaNode> getLastKnownELR() { return lastKnownELR; }
  public CompactArray<ReplicaNode> getOfflineReplicas() {
    return offlineReplicas;
  }
  public TagBuffer getTagBuffer() { return tagBuffer; }
  public void setErrorCode(short errorCode) { this.errorCode = errorCode; }
  public void setPartitionIndex(int partitionIndex) {
    this.partitionIndex = partitionIndex;
  }
  public void setLeaderID(int leaderID) { this.leaderID = leaderID; }
  public void setLeaderEpoch(int leaderEpoch) {
    this.leaderEpoch = leaderEpoch;
  }
  public void setReplicaNodeArray(CompactArray<ReplicaNode> replicaNodeArray) {
    this.replicaNodeArray = replicaNodeArray;
  }
  public void setIsrNodeArray(CompactArray<ReplicaNode> isrNodeArray) {
    this.isrNodeArray = isrNodeArray;
  }
  public void
  setEligibleLeaderReplicas(CompactArray<ReplicaNode> eligibleLeaderReplicas) {
    this.eligibleLeaderReplicas = eligibleLeaderReplicas;
  }
  public void setLastKnownELR(CompactArray<ReplicaNode> lastKnownELR) {
    this.lastKnownELR = lastKnownELR;
  }
  public void setOfflineReplicas(CompactArray<ReplicaNode> offlineReplicas) {
    this.offlineReplicas = offlineReplicas;
  }
  public void setTagBuffer(TagBuffer tagBuffer) { this.tagBuffer = tagBuffer; }
}