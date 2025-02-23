package log;
import java.nio.ByteBuffer;
import java.util.UUID;
import shared.CompactArray;
import shared.VarInt;
import shared.serializer.IntegerSerializer;
import shared.serializer.UUIDSerializer;
import util.StreamUtils;
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
    this.length = VarInt.fromByteBuffer(data);
    this.frameVersion = data.get();
    this.type = data.get();
    this.version = data.get();
    this.partitionID = data.getInt();
    this.topicUUID = new UUID(data.getLong(), data.getLong());
    this.replicaArray = CompactArray.fromByteBuffer(data, integerSerializer);
    this.inSyncReplicaArray =
        CompactArray.fromByteBuffer(data, integerSerializer);
    this.removingReplicasArray =
        CompactArray.fromByteBuffer(data, integerSerializer);
    this.addingReplicasArray =
        CompactArray.fromByteBuffer(data, integerSerializer);
    this.leader = data.getInt();
    this.leaderEpoch = data.getInt();
    this.partitionEpoch = data.getInt();
    this.directoriesArray =
        CompactArray.fromByteBuffer(data, new UUIDSerializer());
    this.taggedFieldsCount = VarInt.fromByteBuffer(data);
  }
  @Override
  public byte[] toBytes() {
    return StreamUtils.toBytes(dos -> {
      dos.write(this.length.toBytes());
      dos.write(this.frameVersion);
      dos.write(this.type);
      dos.write(this.version);
      dos.writeInt(this.partitionID);
      dos.writeLong(this.topicUUID.getMostSignificantBits());
      dos.writeLong(this.topicUUID.getLeastSignificantBits());
      dos.write(this.replicaArray.toBytes());
      dos.write(this.inSyncReplicaArray.toBytes());
      dos.write(this.removingReplicasArray.toBytes());
      dos.write(this.addingReplicasArray.toBytes());
      dos.writeInt(this.leader);
      dos.writeInt(this.leaderEpoch);
      dos.writeInt(this.partitionEpoch);
      dos.write(this.directoriesArray.toBytes());
      dos.write(this.taggedFieldsCount.toBytes());
    });
  }
}