package shared.serializer;

import shared.CompactArray;
import shared.RequestTopicElement;
import shared.TagBuffer;
import util.StreamUtils;

import java.nio.ByteBuffer;
import java.util.UUID;

public class RequestTopicElementSerializer implements ElementSerializer<RequestTopicElement> {
    @Override
    public byte[] toBytes(RequestTopicElement element) {
        return StreamUtils.toBytes(dos -> {
            dos.writeLong(element.getTopicID().getMostSignificantBits());
            dos.writeLong(element.getTopicID().getLeastSignificantBits());
            dos.write(element.getPartitions().toBytes());
            dos.write(element.getTg().toBytes());
        });
    }

    @Override
    public RequestTopicElement fromByteBuffer(ByteBuffer data) {
        UUID topicID = new UUID(data.getLong(), data.getLong());
        return new RequestTopicElement(
                topicID,
                CompactArray.fromByteBuffer(data, new RequestPartitionSerializer()),
                TagBuffer.fromByteBuffer(data));
    }
}