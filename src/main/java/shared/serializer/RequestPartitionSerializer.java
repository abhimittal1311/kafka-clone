package shared.serializer;

import shared.RequestPartition;
import util.StreamUtils;

import java.nio.ByteBuffer;

public class RequestPartitionSerializer implements ElementSerializer<RequestPartition> {
    @Override
    public byte[] toBytes(RequestPartition element) {
        return StreamUtils.toBytes(dos -> {
            dos.writeInt(element.getPartition());
            dos.writeInt(element.getCurrentLeaderEpoch());
            dos.writeLong(element.getFetchOffset());
            dos.writeInt(element.getLastFetchedEpoch());
            dos.writeLong(element.getLogStartOffset());
            dos.writeInt(element.getPartitionMaxBytes());
        });
    }

    @Override
    public RequestPartition fromByteBuffer(ByteBuffer data) {
        return new RequestPartition(data.getInt(), data.getInt(), data.getLong(), data.getInt(), data.getLong(), data.getInt());
    }
}