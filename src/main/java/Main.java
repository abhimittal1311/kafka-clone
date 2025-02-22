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
            // Since the tester restarts your program quite often, setting
            // SO_REUSEADDR ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);

            // Wait for connection from client
            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     InputStream in = clientSocket.getInputStream();
                     OutputStream out = clientSocket.getOutputStream()) {

                    System.err.println("Client connected.");

                    // Handle multiple requests from the same client
                    while (true) {
                        try {
                            // Read the first 4 bytes for message_size
                            byte[] sizeBytes = in.readNBytes(4);
                            if (sizeBytes.length < 4) {
                                System.err.println("Client disconnected.");
                                break; // Exit the loop if the client disconnects
                            }

                            // Read the request header
                            byte[] apiKey = in.readNBytes(2); // API key (2 bytes)
                            byte[] apiVersionBytes = in.readNBytes(2); // API version (2 bytes)
                            short apiVersion = ByteBuffer.wrap(apiVersionBytes).getShort();
                            byte[] correlationId = in.readNBytes(4); // Correlation ID (4 bytes)

                            // Build the response
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();

                            // Write correlation_id (4 bytes)
                            bos.write(correlationId);

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
                            byte[] responseSizeBytes = ByteBuffer.allocate(4).putInt(response.length).array();

                            // Send the response
                            out.write(responseSizeBytes);
                            out.write(response);
                            out.flush();

                            System.err.println("Response sent: " + Arrays.toString(response));
                        } catch (IOException e) {
                            System.err.println("IOException: " + e.getMessage());
                            break; // Exit the loop if an error occurs
                        }
                    }
                } catch (IOException e) {
                    System.err.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}