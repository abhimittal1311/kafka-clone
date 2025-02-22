package shared;

import util.StreamUtils;

import java.nio.ByteBuffer;

public class VarInt {
    private int value;

    public VarInt(int val) {
        this.value = val;
    }

    public int getUnsignedValue() {
        return value;
    }

    public int getValue() {
        return (value >>> 1) ^ -(value & 1);
    }

    public void setUnsignedValue(int value) {
        this.value = value;
    }

    public void setValue(int value) {
        this.value = (value << 1) ^ (value >> 31);
    }

    public static VarInt fromByteBuffer(ByteBuffer data) {
        int accumulator = 0, shift = 0;
        while (true) {
            if (!data.hasRemaining()) {
                throw new IllegalArgumentException("Malformed UNSIGNED_VARINT: insufficient bytes");
            }
            byte b = data.get();
            accumulator |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                //keep getting bytes until MSB is unset.
                break;
            }
            shift += 7;
            if (shift > 35) {
                throw new IllegalArgumentException("Malformed UNSIGNED_VARINT: exceeds 5 bytes");
            }
        }
        return new VarInt(accumulator);
    }

    public byte[] toBytes() {
        return StreamUtils.toBytes(dos -> {
                    int val = this.value;
                    while ((val & ~0x7F) != 0) {
                        dos.write((byte) ((val & 0x7F) | 0x80));
                        val >>>= 7;
                    }
                    dos.write((byte) (val & 0x7F));
                }
        );
    }
}