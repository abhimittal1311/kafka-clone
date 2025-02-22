import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {
    private static final int UNSUPPORTED_VERSION_ERROR_CODE = 35;
    private static final int NO_ERROR_CODE = 0;
    private static final int API_VERSIONS_KEY = 18;
    private static final int DESCRIBE_TOPIC_PARTITIONS_KEY = 75;
    private static final int SUPPORTED_API_VERSION_MIN = 0;
    private static final int SUPPORTED_API_VERSION_MAX = 4;
    private static final int PORT = 9092;

    public static void main(String[] args) {
        System.out.println("kafka server started");

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            // Since the tester restarts your program quite often, setting
            // SO_REUSEADDR ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);

            // Continuously accept client connections
            while (true) {
                // Wait for a new client connection
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getInetAddress());

                // Handle the client connection in a separate thread
                ClientHandler clientHandler = new ClientHandler(clientSocket);
                new Thread(clientHandler).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void sendErrorResponse(OutputStream out, int correlationId) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.err.println("CorrelationID: " + correlationId);
        bos.write(ByteBuffer.allocate(4).putInt(correlationId).array()); // Correlation ID
        bos.write(new byte[]{0, (byte) UNSUPPORTED_VERSION_ERROR_CODE}); // Error code (35)
        int size = bos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array()); // Message size
        out.write(bos.toByteArray()); // Payload
        out.flush();
        System.err.printf("Correlation ID: %d - Sent Error Response with Code: %d%n", correlationId, UNSUPPORTED_VERSION_ERROR_CODE);
    }

    private static void sendAPIVersionsResponse(OutputStream out, int correlationId) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.err.println("CorrelationID: " + correlationId);
        bos.write(ByteBuffer.allocate(4).putInt(correlationId).array()); // Correlation ID
        bos.write(new byte[]{0, (byte) NO_ERROR_CODE}); // No error
        bos.write(3); // Number of API keys (non-standard, 1 byte)
        bos.write(new byte[]{0, (byte) API_VERSIONS_KEY}); // API key 18 (APIVersions)
        bos.write(new byte[]{0, (byte) SUPPORTED_API_VERSION_MIN}); // Min version
        bos.write(new byte[]{0, (byte) SUPPORTED_API_VERSION_MAX}); // Max version
        bos.write(0); // Tagged fields
        bos.write(new byte[]{0, (byte) DESCRIBE_TOPIC_PARTITIONS_KEY}); // API key 75 (DescribeTopicPartitions)
        bos.write(new byte[]{0, 0}); // Min version
        bos.write(new byte[]{0, 0}); // Max version
        bos.write(0); // Tagged fields
        bos.write(new byte[]{0, 0, 0, 0}); // Throttle time
        bos.write(0); // Tagged fields end byte
        int size = bos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array()); // Message size
        out.write(bos.toByteArray()); // Payload
        out.flush();
        System.err.printf("Correlation ID: %d - Sent APIVersions response with no error.%n", correlationId);
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try (DataInputStream in = new DataInputStream(clientSocket.getInputStream());
                 OutputStream out = clientSocket.getOutputStream()) {

                // Handle multiple requests from the same client
                while (true) {
                    try {
                        // Read the request message
                        int incomingMessageSize = in.readInt();
                        byte[] requestApiKeyBytes = in.readNBytes(2);
                        short requestApiVersion = in.readShort();
                        byte[] correlationIdBytes = in.readNBytes(4);
                        byte[] remainingBytes = new byte[incomingMessageSize - 8];
                        in.readFully(remainingBytes);

                        // Extract correlation ID
                        int correlationId = ByteBuffer.wrap(correlationIdBytes).getInt();

                        // Handle the request based on API key and version
                        if (requestApiVersion < SUPPORTED_API_VERSION_MIN || requestApiVersion > SUPPORTED_API_VERSION_MAX) {
                            sendErrorResponse(out, correlationId);
                        } else {
                            sendAPIVersionsResponse(out, correlationId);
                        }
                    } catch (IOException e) {
                        System.out.println("Client disconnected or error occurred: " + e.getMessage());
                        break; // Exit the loop if the client disconnects or an error occurs
                    }
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.out.println("IOException while closing client socket: " + e.getMessage());
                }
            }
        }
    }
}