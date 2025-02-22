package log;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
public class RecordBatch {
  private long baseOffset;
  private int batchLength;
  private int partitionLeaderEpoch;
  private byte magicByte;
  private int crc;
  private short attributes;
  private int lastOffsetDelta;
  private long baseTimestamp;
  private long maxTimestamp;
  private long producerID;
  private short producerEpoch;
  private int baseSequence;
  private int recordsLength;
  private List<Record> records;
  private RecordBatch() {}
  public void setBaseOffset(long baseOffset) { this.baseOffset = baseOffset; }
  public void setBatchLength(int batchLength) {
    this.batchLength = batchLength;
  }
  public void setPartitionLeaderEpoch(int partitionLeaderEpoch) {
    this.partitionLeaderEpoch = partitionLeaderEpoch;
  }
  public void setMagicByte(byte magicByte) { this.magicByte = magicByte; }
  public void setCrc(int crc) { this.crc = crc; }
  public void setAttributes(short attributes) { this.attributes = attributes; }
  public void setLastOffsetDelta(int lastOffsetDelta) {
    this.lastOffsetDelta = lastOffsetDelta;
  }
  public void setBaseTimestamp(long baseTimestamp) {
    this.baseTimestamp = baseTimestamp;
  }
  public void setMaxTimestamp(long maxTimestamp) {
    this.maxTimestamp = maxTimestamp;
  }
  public void setProducerID(long producerID) { this.producerID = producerID; }
  public void setProducerEpoch(short producerEpoch) {
    this.producerEpoch = producerEpoch;
  }
  public void setBaseSequence(int baseSequence) {
    this.baseSequence = baseSequence;
  }
  public void setRecordsLength(int recordsLength) {
    this.recordsLength = recordsLength;
  }
  public void setRecords(List<Record> records) { this.records = records; }
  public long getBaseOffset() { return baseOffset; }
  public int getBatchLength() { return batchLength; }
  public int getPartitionLeaderEpoch() { return partitionLeaderEpoch; }
  public byte getMagicByte() { return magicByte; }
  public int getCrc() { return crc; }
  public short getAttributes() { return attributes; }
  public int getLastOffsetDelta() { return lastOffsetDelta; }
  public long getBaseTimestamp() { return baseTimestamp; }
  public long getMaxTimestamp() { return maxTimestamp; }
  public long getProducerID() { return producerID; }
  public short getProducerEpoch() { return producerEpoch; }
  public int getBaseSequence() { return baseSequence; }
  public List<Record> getRecords() { return records; }
  public int getRecordsLength() { return recordsLength; }
  public static RecordBatch fromByteBuffer(ByteBuffer data) {
    RecordBatch recordBatch = new RecordBatch();
    recordBatch.setBaseOffset(data.getLong());
    recordBatch.setBatchLength(data.getInt());
    int start = data.position();
    recordBatch.setPartitionLeaderEpoch(data.getInt());
    recordBatch.setMagicByte(data.get());
    recordBatch.setCrc(data.getInt());
    recordBatch.setAttributes(data.getShort());
    recordBatch.setLastOffsetDelta(data.getInt());
    recordBatch.setBaseTimestamp(data.getLong());
    recordBatch.setMaxTimestamp(data.getLong());
    recordBatch.setProducerID(data.getLong());
    recordBatch.setProducerEpoch(data.getShort());
    recordBatch.setBaseSequence(data.getInt());
    int recordsLen = data.getInt();
    recordBatch.setRecordsLength(recordsLen);
    List<Record> records = new ArrayList<>(recordsLen);
    for (int i = 0; i < recordsLen; i++) {
      records.add(Record.fromByteBuffer(data));
    }
    recordBatch.setRecords(records);
    int actualLen = data.position() - start;
    if (recordBatch.getBatchLength() != actualLen) {
      throw new IllegalArgumentException(
          "RecordBatch length was: " + actualLen +
          " expected: " + recordBatch.getBatchLength());
    }
    return recordBatch;
  }
}