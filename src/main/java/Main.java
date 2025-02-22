import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
public class Main {
  public static void main(String[] args) {
    System.out.println("fafka server started");
    ServerSocket serverSocket;
    Socket clientSocket = null;
    int port = 9092;
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting
      // SO_REUSEADDR ensures that we don't run into 'Address already in use'
      // errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      clientSocket = serverSocket.accept();
      DataInputStream in = new DataInputStream(clientSocket.getInputStream());
      OutputStream out = clientSocket.getOutputStream();
      while (true) {
        // Handle single request.
        handleRequest(in, out);
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
Expand 7 lines
    }
  }
  private static void handleRequest(DataInputStream inputStream,
                                    OutputStream outputStream)
      throws IOException {
    // read request message
    int incomingMessageSize = inputStream.readInt();
    byte[] requestApiKeyBytes = inputStream.readNBytes(2);
    short requestApiVersion = inputStream.readShort();
    byte[] correlationIdBytes = inputStream.readNBytes(4);
    byte[] remainingBytes = new byte[incomingMessageSize - 8];
    inputStream.readFully(remainingBytes);
    // build message output
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    // .ResponseHeader
    //   .correlation_id
    byteArrayStream.write(correlationIdBytes);
    // .ResponseBody
    //   .error_code
    byteArrayStream.write(getErrorCode(requestApiVersion));
    //   .num_api_keys
    byteArrayStream.write(new byte[] {0, 2});
    //   .api_key
    byteArrayStream.write(requestApiKeyBytes);
    //   .min_version
    byteArrayStream.write(new byte[] {0, 0}); // min v0 INT16
    //   .max_version
    byteArrayStream.write(new byte[] {0, 4}); // max v4 INT16
    //   .TAG_BUFFER
    byteArrayStream.write(new byte[] {0});
    //   .throttle_time_ms
    byteArrayStream.write(new byte[] {0, 0, 0, 0});
    //   .TAG_BUFFER
    byteArrayStream.write(new byte[] {0});
    // write response message
    byte[] bytes = byteArrayStream.toByteArray();
    outputStream.write(ByteBuffer.allocate(4).putInt(bytes.length).array());
    outputStream.write(bytes);
  }
  // error code is two bytes unless there is no error, where a single byte 0 is
  // returned
  private static byte[] getErrorCode(short requestApiVersion) {
    if (requestApiVersion < 0 || requestApiVersion > 4) {
      return ByteBuffer.allocate(2).putShort((short)35).array();
    } else {
      return new byte[] {0};
    }
  }