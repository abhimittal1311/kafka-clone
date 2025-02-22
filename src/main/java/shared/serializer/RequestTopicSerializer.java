package shared.serializer;

import shared.CompactString;
import shared.RequestTopic;
import shared.TagBuffer;
import util.StreamUtils;

import java.nio.ByteBuffer;

public class RequestTopicSerializer implements ElementSerializer<RequestTopic>{


    @Override
    public byte[] toBytes(RequestTopic requestTopic) {
        return StreamUtils.toBytes(dos -> {
            dos.write(requestTopic.getTopicName().toBytes());
            dos.write(requestTopic.getTopicTagBuffer().toBytes());
        });
    }

    @Override
    public RequestTopic fromByteBuffer(ByteBuffer data) {
        return new RequestTopic(CompactString.fromByteBuffer(data), TagBuffer.fromByteBuffer(data));
    }}