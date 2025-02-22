package shared.serializer;

import util.StreamUtils;

import java.nio.ByteBuffer;

public class IntegerSerializer implements ElementSerializer<Integer> {
    @Override
    public byte[] toBytes(Integer element) {
        return StreamUtils.toBytes(dos -> {
            dos.writeInt(element);
        });
    }

    @Override
    public Integer fromByteBuffer(ByteBuffer data) {
        return data.getInt();
    }
}