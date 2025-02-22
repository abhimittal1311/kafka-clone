package responses;

import shared.TagBuffer;
import util.StreamUtils;

import java.nio.ByteBuffer;

public class ResponseHeader {
    private int correlationId;
    private TagBuffer tagBuffer = null;

    public ResponseHeader(int correlationId) {
        this.correlationId = correlationId;
    }

    public ResponseHeader(int correlationId, TagBuffer tagBuffer) {
        this.correlationId = correlationId;
        this.tagBuffer = tagBuffer;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public TagBuffer getTagBuffer() {
        return tagBuffer;
    }

    public void setTagBuffer(TagBuffer tagBuffer) {
        this.tagBuffer = tagBuffer;
    }

    public byte[] toBytes() {
        return StreamUtils.toBytes(dos -> {
            dos.writeInt(this.correlationId);
            if (this.tagBuffer != null) {
                dos.write(this.tagBuffer.toBytes());
            }
        });
    }

    public static ResponseHeader fromByteBuffer(ByteBuffer data) {
        return new ResponseHeader(data.getInt(), TagBuffer.fromByteBuffer(data));
    }
}