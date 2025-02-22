package shared.serializer;

import java.nio.ByteBuffer;

public interface ElementSerializer<T> {
    byte[] toBytes(T element);

    T fromByteBuffer(ByteBuffer data);
}