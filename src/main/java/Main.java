import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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

                    // Handle the client request
                    handleClientRequest(inputStream, outputStream);

                } catch (IOException e) {
                    System.err.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }

    private static void handleClientRequest(InputStream inputStream, OutputStream outputStream) throws IOException {
        // Read the first 4 bytes for message_size
        byte[] sizeBuffer = new byte[4];
        int bytesRead = inputStream.read(sizeBuffer);
        if (bytesRead < 4) {
            System.err.println("Error: Insufficient data for message_size.");
            return;
        }

        // Extract message_size (4 bytes)
        ByteBuffer sizeBufferWrapper = ByteBuffer.wrap(sizeBuffer);
        sizeBufferWrapper.order(ByteOrder.BIG_ENDIAN);
        int messageSize = sizeBufferWrapper.getInt();  // message_size (4 bytes)
        System.err.println("Received message_size: " + messageSize);

        // Read the rest of the header (request_api_key, request_api_version, correlation_id)
        byte[] headerBuffer = new byte[10]; // 2 bytes API key + 2 bytes API version + 4 bytes correlation_id
        bytesRead = inputStream.read(headerBuffer);
        if (bytesRead < 10) {
            System.err.println("Error: Insufficient data for header.");
            return;
        }

        // Extract values from the header (API key, API version, correlation_id)
        ByteBuffer requestBuffer = ByteBuffer.wrap(headerBuffer);
        requestBuffer.order(ByteOrder.BIG_ENDIAN);
        short apiKey = requestBuffer.getShort(); // request_api_key (2 bytes)
        short apiVersion = requestBuffer.getShort(); // request_api_version (2 bytes)
        int correlationId = requestBuffer.getInt(); // correlation_id (4 bytes)

        System.err.println("Received API Key: " + apiKey);
        System.err.println("Received API Version: " + apiVersion);
        System.err.println("Received correlation_id: " + correlationId);

        // Handle the APIVersions request
        if (apiKey == 18) { // API key 18 is APIVersions
            ByteBuffer response = handleApiVersions(apiVersion, correlationId);
            if (response != null) {
                outputStream.write(response.array(), 0, response.limit());
                outputStream.flush();
                System.err.println("Sent APIVersions response with correlation_id: " + correlationId);
            }
        } else {
            System.err.println("Unsupported API key: " + apiKey);
        }
    }

    private static ByteBuffer handleApiVersions(int version, int correlationId) {
        // Calculate the total size of the response
        int totalResponseSize = 4 + // message_size (4 bytes)
                4 + // correlation_id (4 bytes)
                2 + // error_code (2 bytes)
                4 + // throttle_time_ms (4 bytes)
                4 + // array length (4 bytes)
                6; // API key 18 entry (6 bytes: 2 + 2 + 2)

        // Allocate a buffer for the response
        ByteBuffer responseBuffer = ByteBuffer.allocate(totalResponseSize);
        responseBuffer.order(ByteOrder.BIG_ENDIAN);

        // Write message_size (4 bytes)
        responseBuffer.putInt(totalResponseSize - 4); // Exclude the message_size field itself

        // Write correlation_id (4 bytes)
        responseBuffer.putInt(correlationId);

        // Write error_code (2 bytes)
        responseBuffer.putShort((short) 0); // No error

        // Write throttle_time_ms (4 bytes)
        responseBuffer.putInt(0); // throttle_time_ms (set to 0)

        // Write array length (4 bytes)
        responseBuffer.putInt(1); // Array length is 1

        // Write API key 18 entry (6 bytes)
        responseBuffer.putShort((short) 18); // API key 18 (API_VERSIONS)
        responseBuffer.putShort((short) 0); // Min version
        responseBuffer.putShort((short) 4); // Max version

        // Prepare the response for writing
        responseBuffer.flip();
        return responseBuffer;
    }
}