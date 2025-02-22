package shared.serializer;

import shared.CompactArray;
import shared.Partition;
import shared.ReplicaNode;
import shared.TagBuffer;
import util.StreamUtils;

import java.nio.ByteBuffer;

public class PartitionSerializer implements ElementSerializer<Partition> {
    @Override
    public byte[] toBytes(Partition part) {
        return StreamUtils.toBytes(dos -> {
            dos.writeShort(part.getErrorCode());
            dos.writeInt(part.getPartitionIndex());
            dos.writeInt(part.getLeaderID());
            dos.writeInt(part.getLeaderEpoch());
            dos.write(part.getReplicaNodeArray().toBytes());
            dos.write(part.getIsrNodeArray().toBytes());
            dos.write(part.getEligibleLeaderReplicas().toBytes());
            dos.write(part.getLastKnownELR().toBytes());
            dos.write(part.getOfflineReplicas().toBytes());
            dos.write(part.getTagBuffer().toBytes());
        });
    }

    @Override
    public Partition fromByteBuffer(ByteBuffer data) {
        ElementSerializer<ReplicaNode> replicaNodeSerializer = new ReplicaNodeSerializer();
        Partition part = new Partition();
        part.setErrorCode(data.getShort());
        part.setPartitionIndex(data.getInt());
        part.setLeaderID(data.getInt());
        part.setLeaderEpoch(data.getInt());
        part.setReplicaNodeArray(CompactArray.fromByteBuffer(data, replicaNodeSerializer, ReplicaNode::new));
        part.setIsrNodeArray(CompactArray.fromByteBuffer(data, replicaNodeSerializer, ReplicaNode::new));
        part.setEligibleLeaderReplicas(CompactArray.fromByteBuffer(data, replicaNodeSerializer, ReplicaNode::new));
        part.setLastKnownELR(CompactArray.fromByteBuffer(data, replicaNodeSerializer, ReplicaNode::new));
        part.setOfflineReplicas(CompactArray.fromByteBuffer(data, replicaNodeSerializer, ReplicaNode::new));
        part.setTagBuffer(TagBuffer.fromByteBuffer(data));
        return part;
    }
}