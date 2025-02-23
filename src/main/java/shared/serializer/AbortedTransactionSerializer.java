package shared.serializer;
import java.nio.ByteBuffer;
import shared.AbortedTransaction;
import shared.TagBuffer;
import util.StreamUtils;
public class AbortedTransactionSerializer
    implements ElementSerializer<AbortedTransaction> {
  @Override
  public byte[] toBytes(AbortedTransaction element) {
    return StreamUtils.toBytes(dos -> {
      dos.writeLong(element.getProducerId());
      dos.writeLong(element.getFirstOffset());
      dos.write(element.getTg().toBytes());
    });
  }
  @Override
  public AbortedTransaction fromByteBuffer(ByteBuffer data) {
    return new AbortedTransaction(data.getLong(), data.getLong(),
                                  TagBuffer.fromByteBuffer(data));
  }
}