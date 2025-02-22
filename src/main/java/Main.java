import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        System.err.println("Starting server...");
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;

        try {
            serverSocket = new ServerSocket(port);
            // Allow socket reuse to avoid 'Address already in use' errors
            serverSocket.setReuseAddress(true);

            // Wait for client connection
            clientSocket = serverSocket.accept();

            // Get input and output streams
            InputStream in = clientSocket.getInputStream();
            OutputStream out = clientSocket.getOutputStream();

            // Read request size (4 bytes)
            byte[] sizeBuffer = new byte[4];
            in.read(sizeBuffer);
            int messageSize = ByteBuffer.wrap(sizeBuffer).getInt();
            System.err.println("Received message size: " + messageSize);

            // Read the request header (API key, version, and correlation ID)
            byte[] headerBuffer = new byte[10]; // API key (2 bytes) + API version (2 bytes) + correlation ID (4 bytes)
            in.read(headerBuffer);
            ByteBuffer header = ByteBuffer.wrap(headerBuffer);
            short apiKey = header.getShort();
            short apiVersion = header.getShort();
            int correlationId = header.getInt();

            System.err.println("Received API Key: " + apiKey);
            System.err.println("Received API Version: " + apiVersion);
            System.err.println("Received Correlation ID: " + correlationId);

            // Prepare response
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            // Handle unsupported API version (error code 35)
            if (apiVersion < 0 || apiVersion > 4) {
                bos.write(new byte[] {0, 35}); // Error code 35 (UNSUPPORTED_VERSION)
            } else {
                bos.write(new byte[] {0, 0}); // Error code 0 (no error)
                bos.write(1); // Number of API versions in the response (1 entry)
                bos.write(new byte[] {0, 18}); // API key (18)
                bos.write(new byte[] {0, 3}); // Min version (3)
                bos.write(new byte[] {0, 4}); // Max version (4)
                bos.write(new byte[] {0, 0, 0, 0}); // Throttle time (0)
            }

            // Calculate the total size of the response
            byte[] responseBytes = bos.toByteArray();
            int totalSize = 4 + 4 + responseBytes.length; // 4 bytes for message size + 4 bytes for correlation ID + response data

            // Prepare final response
            ByteArrayOutputStream finalResponse = new ByteArrayOutputStream();
            finalResponse.write(ByteBuffer.allocate(4).putInt(totalSize).array()); // Message size (4 bytes)
            finalResponse.write(ByteBuffer.allocate(4).putInt(correlationId).array()); // Correlation ID (4 bytes)
            finalResponse.write(responseBytes); // Response body

            // Send the response
            out.write(finalResponse.toByteArray());
            out.flush();
            System.err.println("Sent response with Correlation ID: " + correlationId);

        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.err.println("IOException during socket close: " + e.getMessage());
            }
        }
    }
}
