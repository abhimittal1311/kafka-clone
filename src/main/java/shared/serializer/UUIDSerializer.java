package shared.serializer;

import util.StreamUtils;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UUIDSerializer implements ElementSerializer<UUID> {
    @Override
    public byte[] toBytes(UUID element) {
        return StreamUtils.toBytes(dos -> {
            dos.writeLong(element.getMostSignificantBits());
            dos.writeLong(element.getLeastSignificantBits());
        });
    }

    @Override
    public UUID fromByteBuffer(ByteBuffer data) {
        return new UUID(data.getLong(), data.getLong());
    }
}