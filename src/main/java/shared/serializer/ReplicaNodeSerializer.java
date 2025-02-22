package shared.serializer;

import shared.ReplicaNode;
import util.StreamUtils;

import java.nio.ByteBuffer;

public class ReplicaNodeSerializer implements ElementSerializer<ReplicaNode> {
    @Override
    public byte[] toBytes(ReplicaNode element) {
        return StreamUtils.toBytes(dos -> {
            dos.writeInt(element.getNodeId());
        });
    }

    @Override
    public ReplicaNode fromByteBuffer(ByteBuffer data) {
        return new ReplicaNode(data.getInt());
    }
}