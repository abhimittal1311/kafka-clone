package shared;

import util.StreamUtils;

import java.nio.ByteBuffer;

public class TagBuffer {
    private byte[] buffer = null;

    public TagBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    public TagBuffer() {
    }

    public byte[] toBytes() {
        return StreamUtils.toBytes(dos -> {
            if (buffer != null && buffer.length != 0) {
                dos.write(new VarInt(buffer.length + 1).toBytes());
                dos.write(buffer);
            } else {
                dos.write(new VarInt(0).toBytes());
            }
        });
    }

    public static TagBuffer fromByteBuffer(ByteBuffer data) {
        int tagLen = VarInt.fromByteBuffer(data).getUnsignedValue();
        if (tagLen == 0) {
            return new TagBuffer();
        }
        byte[] temp = new byte[tagLen - 1];
        data.get(temp);
        return new TagBuffer(temp);
    }
}