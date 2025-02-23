package shared.serializer;

import shared.CompactArray;
import shared.TagBuffer;
import shared.TopicResponse;
import util.StreamUtils;

import java.nio.ByteBuffer;
import java.util.UUID;

public class TopicResponseSerializer implements ElementSerializer<TopicResponse> {
    @Override
    public byte[] toBytes(TopicResponse element) {
        return StreamUtils.toBytes(dos -> {
            dos.writeLong(element.getTopicID().getMostSignificantBits());
            dos.writeLong(element.getTopicID().getLeastSignificantBits());
            dos.write(element.getPartitions().toBytes());
            dos.write(element.getTg().toBytes());
        });
    }

    @Override
    public TopicResponse fromByteBuffer(ByteBuffer data) {
        return new TopicResponse(
                new UUID(data.getLong(), data.getLong()),
                CompactArray.fromByteBuffer(data, new PartitionResponseSerializer()),
                TagBuffer.fromByteBuffer(data)
        );
    }
}