package shared;
public class ReplicaNode {
  private int nodeId = -1;
  public ReplicaNode() {}
  public ReplicaNode(int nodeId) { this.nodeId = nodeId; }
  public int getNodeId() { return nodeId; }
}