package shared;
public class RequestPartition {
  private int partition;
  private int currentLeaderEpoch;
  private long fetchOffset;
  private int lastFetchedEpoch;
  private long logStartOffset;
  private int partitionMaxBytes;
  public RequestPartition() {}
  public RequestPartition(int partition, int currentLeaderEpoch,
                          long fetchOffset, int lastFetchedEpoch,
                          long logStartOffset, int partitionMaxBytes) {
    this.partition = partition;
    this.currentLeaderEpoch = currentLeaderEpoch;
    this.fetchOffset = fetchOffset;
    this.lastFetchedEpoch = lastFetchedEpoch;
    this.logStartOffset = logStartOffset;
    this.partitionMaxBytes = partitionMaxBytes;
  }
  public int getPartition() { return partition; }
  public int getCurrentLeaderEpoch() { return currentLeaderEpoch; }
  public long getFetchOffset() { return fetchOffset; }
  public int getLastFetchedEpoch() { return lastFetchedEpoch; }
  public long getLogStartOffset() { return logStartOffset; }
  public int getPartitionMaxBytes() { return partitionMaxBytes; }
}