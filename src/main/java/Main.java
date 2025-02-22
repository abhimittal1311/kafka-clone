import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
public class Main {
  public static void main(String[] args) {
    System.err.println("Starting server...");
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
      // Get input stream
      InputStream in = clientSocket.getInputStream();
      // Get output stream
      OutputStream out = clientSocket.getOutputStream();
      // request
      // size 4 bytes
      in.readNBytes(4);
      // RQ header
      // api key 16bit
      var apiKey = in.readNBytes(2);
      // api version 16bit
      var apiVersionBytes = in.readNBytes(2);
      var apiVersion = ByteBuffer.wrap(apiVersionBytes).getShort();
      // correlation id 32bit
      byte[] cId = in.readNBytes(4);
      // client_id nullable string
      // tagged fields nullable
      // response
      var bos = new ByteArrayOutputStream();
      // size 32bit
      // written directly to output stream below
      //            bos.write(new byte[]{0, 0, 0, 0});
      // correlation id 32bit
      bos.write(cId);
      // tagged fields nullable
      //            bos.write(0); // tagged fields
      // request specific data
      // APIVersions (v4)
      if (apiVersion < 0 || apiVersion > 4) {
        // error code 16bit
        bos.write(new byte[] {0, 35});
      } else {
        // error code 16bit
        //    api_key => INT16
        //    min_version => INT16
        //    max_version => INT16
        //  throttle_time_ms => INT32
        bos.write(new byte[] {0, 0});       // error code
        bos.write(2);                       // array size + 1
        bos.write(new byte[] {0, 18});      // api_key
        bos.write(new byte[] {0, 3});       // min version
        bos.write(new byte[] {0, 4});       // max version
        bos.write(0);                       // tagged fields
        bos.write(new byte[] {0, 0, 0, 0}); // throttle time
        // All requests and responses will end with a tagged field buffer.  If
        // there are no tagged fields, this will only be a single zero byte.
        bos.write(0); // tagged fields
      }
      // error message nullable string
      // tagged fields nullable
      int size = bos.size();
      byte[] sizeBytes = ByteBuffer.allocate(4).putInt(size).array();
      var response = bos.toByteArray();
      System.out.println(Arrays.toString(sizeBytes));
      System.out.println(Arrays.toString(response));
      out.write(sizeBytes);
      out.write(response);
      out.flush();
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}