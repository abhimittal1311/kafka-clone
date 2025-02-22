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
                try (Socket clientSocket = serverSocket.accept();
                     InputStream in = clientSocket.getInputStream();
                     OutputStream out = clientSocket.getOutputStream()) {

                    // Handle the client request
                    handleClientRequest(in, out);

                } catch (IOException e) {
                    System.err.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }

    private static void handleClientRequest(InputStream in, OutputStream out) throws IOException {
        // Read the first 4 bytes for message_size
        in.readNBytes(4);

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
        byte[] sizeBytes = ByteBuffer.allocate(4).putInt(response.length).array();

        // Send the response
        out.write(sizeBytes);
        out.write(response);
        out.flush();

        System.err.println("Response sent: " + Arrays.toString(response));
    }
}