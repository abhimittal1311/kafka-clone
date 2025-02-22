import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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

                    // Prepare the response body
                    ByteBuffer responseBuffer = ByteBuffer.allocate(1024); // Arbitrary large enough buffer to hold the response

                    // error_code (2 bytes)
                    responseBuffer.putShort((short) 0); // No error, so error_code is 0

                    // api_versions (length: 4 + (2 + 2 + 2) for each entry)
                    List<ApiVersionEntry> apiVersions = new ArrayList<>();
                    apiVersions.add(new ApiVersionEntry((short) 18, (short) 0, (short) 4)); // API key 18, MinVersion 0, MaxVersion 4

                    // Adding the number of entries (1 entry in this case)
                    responseBuffer.putShort((short) apiVersions.size());

                    // Add each API version entry to the response
                    for (ApiVersionEntry entry : apiVersions) {
                        responseBuffer.putShort(entry.apiKey);  // api_key
                        responseBuffer.putShort(entry.minVersion);  // min_version
                        responseBuffer.putShort(entry.maxVersion);  // max_version
                    }

                    // Calculate the total response size
                    int responseSize = responseBuffer.position() + 4 + 4; // message_size (4 bytes) + correlation_id (4 bytes)
                    ByteBuffer finalResponseBuffer = ByteBuffer.allocate(responseSize);

                    // message_size (4 bytes)
                    finalResponseBuffer.putInt(responseSize);

                    // correlation_id (4 bytes)
                    finalResponseBuffer.putInt(correlationId);

                    // Add the error_code and api_versions to the response
                    finalResponseBuffer.put(responseBuffer.array(), 0, responseBuffer.position());

                    // Send response back
                    outputStream.write(finalResponseBuffer.array());
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

    // Inner class to represent API Version entries in the response
    static class ApiVersionEntry {
        short apiKey;
        short minVersion;
        short maxVersion;

        ApiVersionEntry(short apiKey, short minVersion, short maxVersion) {
            this.apiKey = apiKey;
            this.minVersion = minVersion;
            this.maxVersion = maxVersion;
        }
    }
}