package shared.serializer;

import java.nio.ByteBuffer;

public class ByteArraySerializer implements ElementSerializer<byte[]> {
    @Override
    public byte[] toBytes(byte[] element) {
        return element;
    }

    @Override
    public byte[] fromByteBuffer(ByteBuffer data) {
        return data.array();
    }
}