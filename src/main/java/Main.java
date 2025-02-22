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

                    // Read at least 10 bytes for the request header
                    byte[] headerBuffer = new byte[10];
                    int bytesRead = inputStream.read(headerBuffer);

                    if (bytesRead < 10) {
                        System.err.println("Error: Insufficient data read. Expected 10 bytes, but got " + bytesRead + " bytes.");
                        continue;
                    }

                    // Extract correlation_id (bytes 6 to 10)
                    ByteBuffer requestBuffer = ByteBuffer.wrap(headerBuffer);
                    requestBuffer.position(4); // Skip message_size (4 bytes)
                    short apiKey = requestBuffer.getShort(); // Read API key (2 bytes)
                    short apiVersion = requestBuffer.getShort(); // Read API version (2 bytes)
                    int correlationId = requestBuffer.getInt(); // Read correlation_id (4 bytes)

                    System.err.println("Extracted correlation_id: " + correlationId);

                    // Construct response
                    int messageSize = 8;  // Total size: 4 bytes (message_size) + 4 bytes (correlation_id)

                    ByteBuffer responseBuffer = ByteBuffer.allocate(messageSize);
                    responseBuffer.putInt(messageSize);  // message_size (4 bytes)
                    responseBuffer.putInt(correlationId); // correlation_id (4 bytes)

                    // Send response back
                    outputStream.write(responseBuffer.array());
                    outputStream.flush();
                    System.err.println("Sent response with correlation_id: " + correlationId);

                } catch (IOException e) {
                    System.err.println("IOException: " + e.getMessage());
                } catch (java.nio.BufferUnderflowException e) {
                    System.err.println("BufferUnderflowException: Insufficient data available to read. Please check the request.");
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}
