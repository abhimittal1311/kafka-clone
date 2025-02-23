package shared.serializer;

import shared.CompactArray;
import shared.PartitionResponse;
import shared.TagBuffer;
import util.StreamUtils;

import java.nio.ByteBuffer;

public class PartitionResponseSerializer implements ElementSerializer<PartitionResponse> {
    @Override
    public byte[] toBytes(PartitionResponse element) {
        return StreamUtils.toBytes(dos -> {
            dos.writeInt(element.getPartitionIndex());
            dos.writeShort(element.getErrorCode());
            dos.writeLong(element.getHighWatermark());
            dos.writeLong(element.getLastStableOffset());
            dos.writeLong(element.getLogStartOffset());
            dos.write(element.getAbortedTransactions().toBytes());
            dos.writeInt(element.getPreferredReadReplica());
            dos.write(element.getRecords().toBytes());
            dos.write(element.getTg().toBytes());
        });
    }

    @Override
    public PartitionResponse fromByteBuffer(ByteBuffer data) {
        return new PartitionResponse(
                data.getInt(),
                data.getShort(),
                data.getLong(),
                data.getLong(),
                data.getLong(),
                CompactArray.fromByteBuffer(data, new AbortedTransactionSerializer()),
                data.getInt(),
                CompactArray.fromByteBuffer(data, new ByteArraySerializer()),
                TagBuffer.fromByteBuffer(data)
        );
    }
}