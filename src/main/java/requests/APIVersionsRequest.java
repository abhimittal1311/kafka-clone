package requests;

import shared.CompactString;
import shared.TagBuffer;
import util.StreamUtils;

import java.nio.ByteBuffer;

public class APIVersionsRequest extends RequestBody<APIVersionsRequest> {
    private CompactString clientID;
    private CompactString clientSoftwareVersion;
    private TagBuffer tagBuffer;

    public CompactString getClientID() {
        return clientID;
    }

    public CompactString getClientSoftwareVersion() {
        return clientSoftwareVersion;
    }

    public TagBuffer getTagBuffer() {
        return tagBuffer;
    }

    @Override
    public APIVersionsRequest fromByteBuffer(ByteBuffer data) {
        this.clientID = CompactString.fromByteBuffer(data);
        this.clientSoftwareVersion = CompactString.fromByteBuffer(data);
        this.tagBuffer = TagBuffer.fromByteBuffer(data);
        return this;
    }

    @Override
    public byte[] toBytes() {
        return StreamUtils.toBytes(dos -> {
            dos.write(clientID.toBytes());
            dos.write(clientSoftwareVersion.toBytes());
            dos.write(tagBuffer.toBytes());
        });
    }
}