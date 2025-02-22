import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
public class Main {
  private static final int UNSUPPORTED_VERSION_ERROR_CODE = 35;
  private static final int NO_ERROR_CODE = 0;
  private static final int API_VERSIONS_KEY = 18;
  private static final int SUPPORTED_API_VERSION_MIN = 0;
  private static final int SUPPORTED_API_VERSION_MAX = 4;
  private static final int PORT = 9092;
  private static void sendErrorResponse(OutputStream out, int correlationId)
      throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    System.err.println("CorrelationID: " + correlationId);
    bos.write(
        ByteBuffer.allocate(4).putInt(correlationId).array()); // Correlation ID
    bos.write(new byte[] {
        0, (byte)UNSUPPORTED_VERSION_ERROR_CODE}); // Error code (35)
    int size = bos.size();
    out.write(ByteBuffer.allocate(4).putInt(size).array()); // Message size
    out.write(bos.toByteArray());                           // Payload
    out.flush();
    System.err.printf(
        "Correlation ID: %d - Sent Error Response with Code: %d%n",
        correlationId, UNSUPPORTED_VERSION_ERROR_CODE);
  }
  private static void sendAPIVersionsResponse(OutputStream out,
                                              int correlationId)
      throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    System.err.println("CorrelationID: " + correlationId);
    bos.write(
        ByteBuffer.allocate(4).putInt(correlationId).array()); // Correlation ID
    bos.write(new byte[] {0, (byte)NO_ERROR_CODE});            // No error
    bos.write(3); // Number of API keys
    bos.write(
        new byte[] {0, (byte)API_VERSIONS_KEY}); // API key (API_VERSIONS_KEY)
    bos.write(new byte[] {0, (byte)SUPPORTED_API_VERSION_MIN}); // Min version
    bos.write(new byte[] {0, (byte)SUPPORTED_API_VERSION_MAX}); // Max version
    bos.write(0);
    bos.write(new byte[] {0, 75});
    bos.write(new byte[] {0, 0});
    bos.write(new byte[] {0, 0});
    bos.write(0);
    bos.write(new byte[] {0, 0, 0, 0}); // Throttle time
    bos.write(0);                       // Tagged fields end byte
    int size = bos.size();
    out.write(ByteBuffer.allocate(4).putInt(size).array()); // Message size
    out.write(bos.toByteArray());                           // Payload
    out.flush();
    System.err.printf(
        "Correlation ID: %d - Sent APIVersions response with no error.%n",
        correlationId);
  }
  private static class ClientHandler implements Runnable {
    private final Socket clientSocket;
    public ClientHandler(Socket socket) { this.clientSocket = socket; }
    @Override
    public void run() {
      System.err.println("Client connected: " + clientSocket.getInetAddress());
      try (DataInputStream di =
               new DataInputStream(clientSocket.getInputStream());
           OutputStream out = clientSocket.getOutputStream()) {
        while (true) { //
          // Process multiple requests from the same client
          // Read message length
          int dataLen = di.readInt();
          if (dataLen <= 0) {
            System.err.printf("Invalid message size received: %d%n", dataLen);
            break;
          }
          System.err.printf("Data length: %d%n", dataLen);
          // Read the message data based on length
          byte[] tmp = new byte[dataLen];
          int bytesRead = di.read(tmp);
          if (bytesRead != dataLen) {
            System.err.println("Incomplete message received.");
            break;
          }
          ByteBuffer inputBuf = ByteBuffer.wrap(tmp);
          short apiKey = inputBuf.getShort();
          short apiVersion = inputBuf.getShort();
          int correlationId = inputBuf.getInt();
          System.err.printf(
              "API Key: %d, API Version: %d, Correlation ID: %d%n", apiKey,
              apiVersion, correlationId);
          // Handle the request based on API key and version
          if (apiKey != API_VERSIONS_KEY ||
              apiVersion < SUPPORTED_API_VERSION_MIN ||
              apiVersion > SUPPORTED_API_VERSION_MAX) {
            sendErrorResponse(out, correlationId);
          } else {
            sendAPIVersionsResponse(out, correlationId);
          }
        }
      } catch (IOException e) {
        System.err.println("IOException while handling client: " +
                           e.getMessage());
      } finally {
        try {
          clientSocket.close();
        } catch (IOException e) {
          System.err.println("IOException while closing client socket: " +
                             e.getMessage());
        }
      }
    }
  }
  public static void main(String[] args) {
    System.err.println("Starting server on port " + PORT + "...");
    try (ServerSocket serverSocket = new ServerSocket(PORT)) {
      serverSocket.setReuseAddress(true);
      while (true) { // Accept client connections in a loop
        try {
          Socket clientSocket = serverSocket.accept();
          // Create a new thread for each client connection
          Thread clientThread = new Thread(new ClientHandler(clientSocket));
          clientThread.start();
        } catch (IOException e) {
          System.err.println("IOException while accepting client: " +
                             e.getMessage());
        }
      }
    } catch (IOException e) {
      System.err.println("IOException while starting server: " +
                         e.getMessage());
    }
  }
}