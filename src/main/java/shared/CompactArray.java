package shared;

import shared.serializer.ElementSerializer;
import util.StreamUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CompactArray<T> {
    private final List<T> elements;
    private final ElementSerializer<T> serializer;

    public CompactArray(List<T> elements, ElementSerializer<T> serializer) {
        this.elements = elements;
        this.serializer = serializer;
    }

    public CompactArray(ElementSerializer<T> serializer) {
        this.elements = new ArrayList<>();
        this.serializer = serializer;
    }

    public static <T> CompactArray<T> empty(ElementSerializer<T> serializer) {
        return new CompactArray<>(serializer);
    }

    public static <T> CompactArray<T> withElements(List<T> elements, ElementSerializer<T> serializer) {
        return new CompactArray<>(elements, serializer);
    }

    public List<T> getElements() {
        return elements;
    }

    public byte[] toBytes() {
        if (elements == null) {
            return new byte[]{0};
        }
        return StreamUtils.toBytes(dos -> {
            dos.write(new VarInt(elements.size() + 1).toBytes());
            for (T element : elements) {
                dos.write(serializer.toBytes(element));
            }
        });
    }

    public static <T> CompactArray<T> fromByteBuffer(ByteBuffer data, ElementSerializer<T> serializer) {
        int len = VarInt.fromByteBuffer(data).getUnsignedValue();
        if (len == 0) {
            return new CompactArray<>(serializer);
        }
        len = len - 1;
        List<T> elements = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            elements.add(serializer.fromByteBuffer(data));
        }
        return new CompactArray<>(elements, serializer);
    }


}