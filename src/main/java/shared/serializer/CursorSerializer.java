package shared.serializer;

import shared.CompactString;
import shared.Cursor;
import util.StreamUtils;

import java.nio.ByteBuffer;

public class CursorSerializer implements ElementSerializer<Cursor> {
    @Override
    public byte[] toBytes(Cursor element) {
        return StreamUtils.toBytes(dos -> {
            if (element.isNull()) {
                dos.write(-1);
            } else {
                if (element.getTopicName().isPresent() && element.getPartitionIndex().isPresent()) {
                    dos.write(element.getTopicName().get().toBytes());
                    dos.writeInt(element.getPartitionIndex().get());
                }
            }
        });
    }

    @Override
    public Cursor fromByteBuffer(ByteBuffer data) {
        int pos = data.position();
        if (data.get() == -1) {
            return Cursor.nullCursor();
        } else {
            data.position(pos);
            CompactString str = CompactString.fromByteBuffer(data);
            return new Cursor(str, data.getInt());
        }
    }
}