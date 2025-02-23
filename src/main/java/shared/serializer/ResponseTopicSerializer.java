package shared.serializer;

import shared.CompactArray;
import shared.CompactString;
import shared.ResponseTopic;
import shared.TagBuffer;
import util.StreamUtils;

import java.nio.ByteBuffer;
import java.util.UUID;

public class ResponseTopicSerializer implements ElementSerializer<ResponseTopic> {
    @Override
    public byte[] toBytes(ResponseTopic element) {
        return StreamUtils.toBytes(dos -> {
            UUID uuid = element.getTopicID();
            dos.writeShort(element.getErrorCode());
            dos.write(element.getTopicName().toBytes());
            dos.writeLong(uuid.getMostSignificantBits());
            dos.writeLong(uuid.getLeastSignificantBits());
            dos.writeBoolean(element.isInternal());
            dos.write(element.getPartitionCompactArray().toBytes());
            dos.writeInt(element.getTopicAuthorizedOperations());
            dos.write(element.getTagBuffer().toBytes());
        });
    }

    @Override
    public ResponseTopic fromByteBuffer(ByteBuffer data) {
        ResponseTopic topic = new ResponseTopic();
        topic.setErrorCode(data.getShort());
        topic.setTopicName(CompactString.fromByteBuffer(data));
        topic.setTopicID(new UUID(data.getLong(), data.getLong()));
        topic.setInternal(data.get() != 0);
        topic.setPartitionCompactArray(CompactArray.fromByteBuffer(data, new PartitionSerializer()));
        topic.setTopicAuthorizedOperations(data.getInt());
        topic.setTagBuffer(TagBuffer.fromByteBuffer(data));
        return topic;
    }
}