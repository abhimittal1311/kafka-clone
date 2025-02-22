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

        int port = 9092;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
                    System.err.println("Client connected: " + clientSocket.getInetAddress());

                    // Handle multiple requests from the same client
                    handleClientRequests(clientSocket);

                } catch (IOException e) {
                    System.err.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }

    private static void handleClientRequests(Socket clientSocket) throws IOException {
        try (InputStream in = clientSocket.getInputStream();
             OutputStream out = clientSocket.getOutputStream()) {

            while (true) {
                // Read the first 4 bytes for message_size
                byte[] sizeBuffer = new byte[4];
                int bytesRead = in.read(sizeBuffer);
                if (bytesRead < 4) {
                    System.err.println("Error: Insufficient data for message_size.");
                    break;
                }

                // Extract message_size (4 bytes)
                int messageSize = ByteBuffer.wrap(sizeBuffer).getInt();
                System.err.println("Received message_size: " + messageSize);

                // Read the rest of the header (request_api_key, request_api_version, correlation_id)
                byte[] headerBuffer = new byte[10]; // 2 bytes API key + 2 bytes API version + 4 bytes correlation_id
                bytesRead = in.read(headerBuffer);
                if (bytesRead < 10) {
                    System.err.println("Error: Insufficient data for header.");
                    break;
                }

                // Extract values from the header (API key, API version, correlation_id)
                ByteBuffer requestBuffer = ByteBuffer.wrap(headerBuffer);
                short apiKey = requestBuffer.getShort(); // request_api_key (2 bytes)
                short apiVersion = requestBuffer.getShort(); // request_api_version (2 bytes)
                int correlationId = requestBuffer.getInt(); // correlation_id (4 bytes)

                System.err.println("Received API Key: " + apiKey);
                System.err.println("Received API Version: " + apiVersion);
                System.err.println("Received correlation_id: " + correlationId);

                // Handle the APIVersions request
                ByteArrayOutputStream bos = new ByteArrayOutputStream();

                // Write correlation_id (4 bytes)
                bos.write(headerBuffer, 6, 4); // Reuse correlation_id from the request

                // Handle APIVersions request
                if (apiVersion < 0 || apiVersion > 4) {
                    // Unsupported version, respond with error code 35 (UNSUPPORTED_VERSION)
                    bos.write(new byte[]{0, 35}); // Error code (2 bytes)
                } else {
                    // Valid version, respond with API versions
                    bos.write(new byte[]{0, 0}); // Error code (2 bytes, no error)
                    bos.write(2); // Array size (1 byte, non-standard)
                    bos.write(new byte[]{0, 18}); // API key 18 (API_VERSIONS)
                    bos.write(new byte[]{0, 3}); // Min version
                    bos.write(new byte[]{0, 4}); // Max version
                    bos.write(0); // Tagged fields (1 byte)
                    bos.write(new byte[]{0, 0, 0, 0}); // Throttle time (4 bytes)
                    bos.write(0); // Tagged fields (1 byte)
                }

                // Write the response size (4 bytes)
                byte[] response = bos.toByteArray();
                byte[] sizeBytes = ByteBuffer.allocate(4).putInt(response.length).array();

                // Send the response
                out.write(sizeBytes);
                out.write(response);
                out.flush();

                System.err.println("Response sent: " + Arrays.toString(response));
            }

        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        } finally {
            System.err.println("Client disconnected: " + clientSocket.getInetAddress());
        }
    }
}