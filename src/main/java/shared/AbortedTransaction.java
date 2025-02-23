package shared;
public class AbortedTransaction {
  private long producerId;
  private long firstOffset;
  private TagBuffer tg;
  public AbortedTransaction() {}
  public AbortedTransaction(long producerId, long firstOffset, TagBuffer tg) {
    this.producerId = producerId;
    this.firstOffset = firstOffset;
    this.tg = tg;
  }
  public long getProducerId() { return producerId; }
  public void setProducerId(long producerId) { this.producerId = producerId; }
  public long getFirstOffset() { return firstOffset; }
  public void setFirstOffset(long firstOffset) {
    this.firstOffset = firstOffset;
  }
  public TagBuffer getTg() { return tg; }
  public void setTg(TagBuffer tg) { this.tg = tg; }
}