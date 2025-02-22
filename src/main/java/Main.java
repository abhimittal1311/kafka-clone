import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class Main {
    private static final int UNSUPPORTED_VERSION_ERROR_CODE = 35;
    private static final int NO_ERROR_CODE = 0;
    private static final int UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE = 3;
    private static final int API_VERSIONS_KEY = 18;
    private static final int DESCRIBE_TOPIC_PARTITIONS_KEY = 75;
    private static final int SUPPORTED_API_VERSION_MIN = 0;
    private static final int SUPPORTED_API_VERSION_MAX = 4;
    private static final int PORT = 9092;

    public static void main(String[] args) {
        System.out.println("Kafka server started");

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

    private static void sendDescribeTopicPartitionsResponse(OutputStream out, int correlationId, String topicName) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        // .ResponseHeader
        //   .correlation_id
        bos.write(ByteBuffer.allocate(4).putInt(correlationId).array());

        // .ResponseBody
        //   .error_code
        bos.write(new byte[]{0, (byte) UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE}); // Error code 3 (UNKNOWN_TOPIC_OR_PARTITION)

        //   .topics
        bos.write(1); // Number of topics (1 byte)

        //     .topic_name
        byte[] topicNameBytes = topicName.getBytes(StandardCharsets.UTF_8);
        bos.write(ByteBuffer.allocate(2).putShort((short) topicNameBytes.length).array()); // Topic name length (2 bytes)
        bos.write(topicNameBytes); // Topic name

        //     .topic_id
        UUID topicId = UUID.fromString("00000000-0000-0000-0000-000000000000");
        bos.write(ByteBuffer.allocate(16).putLong(topicId.getMostSignificantBits()).putLong(topicId.getLeastSignificantBits()).array()); // Topic ID (16 bytes)

        //     .partitions
        bos.write(0); // Number of partitions (1 byte, empty array)

        //   .throttle_time_ms
        bos.write(new byte[]{0, 0, 0, 0}); // Throttle time (4 bytes)

        //   .TAG_BUFFER
        bos.write(0); // Tagged fields end byte

        // Write the response message
        byte[] responseBytes = bos.toByteArray();
        out.write(ByteBuffer.allocate(4).putInt(responseBytes.length).array()); // Message size (4 bytes)
        out.write(responseBytes); // Response body
        out.flush();

        System.err.printf("Correlation ID: %d - Sent DescribeTopicPartitions response for topic: %s%n", correlationId, topicName);
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

                        // Handle the request based on API key
                        short apiKey = ByteBuffer.wrap(requestApiKeyBytes).getShort();
                        if (apiKey == API_VERSIONS_KEY) {
                            if (requestApiVersion < SUPPORTED_API_VERSION_MIN || requestApiVersion > SUPPORTED_API_VERSION_MAX) {
                                sendErrorResponse(out, correlationId);
                            } else {
                                sendAPIVersionsResponse(out, correlationId);
                            }
                        } else if (apiKey == DESCRIBE_TOPIC_PARTITIONS_KEY) {
                            // Parse the DescribeTopicPartitions request
                            ByteBuffer requestBuffer = ByteBuffer.wrap(remainingBytes);
                            int topicNameLength = requestBuffer.getShort();
                            byte[] topicNameBytes = new byte[topicNameLength];
                            requestBuffer.get(topicNameBytes);
                            String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);

                            // Send the DescribeTopicPartitions response
                            sendDescribeTopicPartitionsResponse(out, correlationId, topicName);
                        } else {
                            // Unsupported API key
                            sendErrorResponse(out, correlationId);
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