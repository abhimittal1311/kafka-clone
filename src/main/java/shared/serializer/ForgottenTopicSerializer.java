package shared.serializer;

import shared.CompactArray;
import shared.ForgottenTopic;
import shared.TagBuffer;
import util.StreamUtils;

import java.nio.ByteBuffer;
import java.util.UUID;

public class ForgottenTopicSerializer implements ElementSerializer<ForgottenTopic> {
    @Override
    public byte[] toBytes(ForgottenTopic element) {
        return StreamUtils.toBytes(dos -> {
            dos.writeLong(element.getTopicID().getMostSignificantBits());
            dos.writeLong(element.getTopicID().getLeastSignificantBits());
            dos.write(element.getPartitions().toBytes());
            dos.write(element.getTg().toBytes());
        });
    }

    @Override
    public ForgottenTopic fromByteBuffer(ByteBuffer data) {
        return new ForgottenTopic(
                new UUID(data.getLong(), data.getLong()),
                CompactArray.fromByteBuffer(data, new IntegerSerializer()),
                TagBuffer.fromByteBuffer(data)
        );
    }
}