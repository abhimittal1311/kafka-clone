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

                    // Check if the request_api_version is valid (0 to 4)
                    if (apiVersion < 0 || apiVersion > 4) {
                        // Unsupported version, respond with error code 35 (UNSUPPORTED_VERSION)
                        sendErrorResponse(outputStream, correlationId, 35); // Error code 35 for UNSUPPORTED_VERSION
                        continue;
                    }

                    // Handle APIVersions request (API key 18)
                    if (apiKey == 18) {
                        sendApiVersionsResponse(outputStream, correlationId);
                    } else {
                        // For unsupported API keys, respond with error code 1 (UNKNOWN_API_KEY)
                        sendErrorResponse(outputStream, correlationId, 1); // Error code 1 for UNKNOWN_API_KEY
                    }

                } catch (IOException e) {
                    System.err.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }

    // Helper method to send an error response with a specific error code
    private static void sendErrorResponse(OutputStream outputStream, int correlationId, int errorCode) throws IOException {
        // Response size: 4 bytes for message_size + 4 bytes for correlation_id + 2 bytes for error_code
        int responseSize = 10; // 4 (message_size) + 4 (correlation_id) + 2 (error_code)
        ByteBuffer responseBuffer = ByteBuffer.allocate(responseSize);
        
        // message_size (4 bytes)
        responseBuffer.putInt(responseSize);
        
        // correlation_id (4 bytes)
        responseBuffer.putInt(correlationId);
        
        // error_code (2 bytes)
        responseBuffer.putShort((short) errorCode); // Error code 35 for UNSUPPORTED_VERSION
        
        // Send the error response
        outputStream.write(responseBuffer.array());
        outputStream.flush();
        System.err.println("Sent error response with error_code: " + errorCode);
    }

    // Helper method to send a valid APIVersions response
    private static void sendApiVersionsResponse(OutputStream outputStream, int correlationId) throws IOException {
        // Define the API versions
        short apiKey = 18; // API_VERSIONS
        short minVersion = 0; // Minimum version
        short maxVersion = 4; // Maximum version

        // Number of API versions
        short numberOfVersions = 1; // We have one version for API key 18

        // Calculate the response size
        int responseSize = 4 + 4 + 2 + 2 + 2 + 2; // message_size + correlation_id + error_code + number_of_versions + (api_key + min_version + max_version)
        ByteBuffer responseBuffer = ByteBuffer.allocate(responseSize);

        // Fill the response buffer
        responseBuffer.putInt(responseSize); // message_size
        responseBuffer.putInt(correlationId); // correlation_id
        responseBuffer.putShort((short) 0); // error_code (0 for no error)
        responseBuffer.putShort(numberOfVersions); // number of API versions
        responseBuffer.putShort(apiKey); // API key
        responseBuffer.putShort(minVersion); // min_version
        responseBuffer.putShort(maxVersion); // max_version

        // Send the response back
        outputStream.write(responseBuffer.array());
        outputStream.flush();
        System.err.println("Sent APIVersions response with correlation_id: " + correlationId);
    }
}