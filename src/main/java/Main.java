import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");

        int port = 9092;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     InputStream inputStream = clientSocket.getInputStream();
                     OutputStream outputStream = clientSocket.getOutputStream()) {

                    // Read the first 4 bytes for message_size
                    byte[] sizeBuffer = new byte[4];
                    int bytesRead = inputStream.read(sizeBuffer);
                    if (bytesRead < 4) {
                        System.err.println("Error: Insufficient data for message_size.");
                        continue;
                    }

                    // Extract message_size (4 bytes)
                    ByteBuffer sizeBufferWrapper = ByteBuffer.wrap(sizeBuffer);
                    int messageSize = sizeBufferWrapper.getInt();  // message_size (4 bytes)
                    System.err.println("Received message_size: " + messageSize);

                    // Read the rest of the header (request_api_key, request_api_version, correlation_id)
                    byte[] headerBuffer = new byte[10]; // 2 bytes API key + 2 bytes API version + 4 bytes correlation_id
                    bytesRead = inputStream.read(headerBuffer);
                    if (bytesRead < 10) {
                        System.err.println("Error: Insufficient data for header.");
                        continue;
                    }

                    // Extract values from the header (API key, API version, correlation_id)
                    ByteBuffer requestBuffer = ByteBuffer.wrap(headerBuffer);
                    short apiKey = requestBuffer.getShort(); // request_api_key (2 bytes)
                    short apiVersion = requestBuffer.getShort(); // request_api_version (2 bytes)
                    int correlationId = requestBuffer.getInt(); // correlation_id (4 bytes)

                    System.err.println("Received API Key: " + apiKey);
                    System.err.println("Received API Version: " + apiVersion);
                    System.err.println("Received correlation_id: " + correlationId);

                    // Construct response
                    int responseSize = 8;  // 4 bytes for message_size + 4 bytes for correlation_id

                    ByteBuffer responseBuffer = ByteBuffer.allocate(responseSize);
                    responseBuffer.putInt(responseSize);  // message_size (4 bytes)
                    responseBuffer.putInt(correlationId); // correlation_id (4 bytes)

                    // Send response back
                    outputStream.write(responseBuffer.array());
                    outputStream.flush();
                    System.err.println("Sent response with correlation_id: " + correlationId);

                } catch (IOException e) {
                    System.err.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}
