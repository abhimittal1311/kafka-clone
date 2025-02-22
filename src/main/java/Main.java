import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        System.err.println("Starting server...");
        ServerSocket serverSocket;
        Socket clientSocket = null;
        int port = 9092;

        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            clientSocket = serverSocket.accept();

            InputStream in = clientSocket.getInputStream();
            OutputStream out = clientSocket.getOutputStream();

            // Read the message length (4 bytes)
            byte[] lengthBytes = new byte[4];
            if (in.read(lengthBytes) != 4) {
                System.err.println("Failed to read message length");
                return;
            }
            int messageLength = ByteBuffer.wrap(lengthBytes).getInt();

            // Read the request header
            byte[] apiKeyBytes = new byte[2];
            byte[] apiVersionBytes = new byte[2];
            byte[] correlationIdBytes = new byte[4];

            in.read(apiKeyBytes);
            in.read(apiVersionBytes);
            in.read(correlationIdBytes);

            short apiKey = ByteBuffer.wrap(apiKeyBytes).getShort();
            short apiVersion = ByteBuffer.wrap(apiVersionBytes).getShort();
            int correlationId = ByteBuffer.wrap(correlationIdBytes).getInt();

            System.out.println("Received request for API Key: " + apiKey + ", Version: " + apiVersion + ", Correlation ID: " + correlationId);

            // Prepare the response
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.order(ByteOrder.BIG_ENDIAN);

            // Write the correlation ID
            bos.write(correlationIdBytes);

            // Handle API_VERSIONS request
            if (apiKey == 18) { // API_VERSIONS key
                if (apiVersion < 0 || apiVersion > 4) {
                    // Error code for unsupported version
                    bos.write(new byte[]{0, 35}); // Error code 35
                } else {
                    // No error
                    bos.write(new byte[]{0, 0}); // Error code 0
                    bos.write(1); // Number of API versions
                    bos.write(new byte[]{0, 18}); // API key
                    bos.write(new byte[]{0, 0}); // Min version
                    bos.write(new byte[]{0, 4}); // Max version
                    bos.write(0); // Tagged fields
                    bos.write(new byte[]{0, 0, 0, 0}); // Throttle time
                }
            } else {
                // Error code for unknown API key
                bos.write(new byte[]{0, 1}); // Error code 1
            }

            // Prepare the response size
            byte[] responseBytes = bos.toByteArray();
            int responseSize = responseBytes.length + 4; // +4 for the size itself
            byte[] sizeBytes = ByteBuffer.allocate(4).putInt(responseSize).array();

            // Send the response
            out.write(sizeBytes);
            out.write(responseBytes);
            out.flush();
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.err.println("IOException while closing socket: " + e.getMessage());
            }
        }
    }
}