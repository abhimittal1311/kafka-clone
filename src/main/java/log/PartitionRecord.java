package log;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Supplier;
import shared.CompactArray;
import shared.VarInt;
import shared.serializer.IntegerSerializer;
import shared.serializer.UUIDSerializer;
public class PartitionRecord extends ValueRecord {
  private int partitionID;
  private UUID topicUUID;
  private CompactArray<Integer> replicaArray;
  private CompactArray<Integer> inSyncReplicaArray;
  private CompactArray<Integer> removingReplicasArray;
  private CompactArray<Integer> addingReplicasArray;
  private int leader;
  private int leaderEpoch;
  private int partitionEpoch;
  private CompactArray<UUID> directoriesArray;
  protected PartitionRecord() {}
  public int getPartitionID() { return partitionID; }
  public UUID getTopicUUID() { return topicUUID; }
  public CompactArray<Integer> getReplicaArray() { return replicaArray; }
  public CompactArray<Integer> getInSyncReplicaArray() {
    return inSyncReplicaArray;
  }
  public CompactArray<Integer> getRemovingReplicasArray() {
    return removingReplicasArray;
  }
  public CompactArray<Integer> getAddingReplicasArray() {
    return addingReplicasArray;
  }
  public int getLeader() { return leader; }
  public int getLeaderEpoch() { return leaderEpoch; }
  public int getPartitionEpoch() { return partitionEpoch; }
  public CompactArray<UUID> getDirectoriesArray() { return directoriesArray; }
  @Override
  protected void parse(ByteBuffer data) {
    IntegerSerializer integerSerializer = new IntegerSerializer();
    Supplier<Integer> defaultIntValue = () -> - 1;
    Supplier<UUID> defaultUUID =
        () -> UUID.fromString("00000000-0000-0000-0000-000000000000");
    this.frameVersion = data.get();
    this.type = data.get();
    this.version = data.get();
    this.partitionID = data.getInt();
    this.topicUUID = new UUID(data.getLong(), data.getLong());
    this.replicaArray =
        CompactArray.fromByteBuffer(data, integerSerializer, defaultIntValue);
    this.inSyncReplicaArray =
        CompactArray.fromByteBuffer(data, integerSerializer, defaultIntValue);
    this.removingReplicasArray =
        CompactArray.fromByteBuffer(data, integerSerializer, defaultIntValue);
    this.addingReplicasArray =
        CompactArray.fromByteBuffer(data, integerSerializer, defaultIntValue);
    this.leader = data.getInt();
    this.leaderEpoch = data.getInt();
    this.partitionEpoch = data.getInt();
    this.directoriesArray =
        CompactArray.fromByteBuffer(data, new UUIDSerializer(), defaultUUID);
    this.taggedFieldsCount = VarInt.fromByteBuffer(data);
  }
}