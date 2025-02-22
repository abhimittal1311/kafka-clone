import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Main {
    private static final int UNSUPPORTED_VERSION_ERROR_CODE = 35;
    private static final int NO_ERROR_CODE = 0;
    private static final int API_VERSIONS_KEY = 18;
    private static final int SUPPORTED_API_VERSION_MIN = 0;
    private static final int SUPPORTED_API_VERSION_MAX = 4;
    private static final int PORT = 9092;
    private static final int DESCRIBE_TOPIC_PARTITIONS_KEY = 75;
    private static final String CLUSTER_METADATA_LOG_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

    /**
     * Parses the __cluster_metadata log file to extract topic metadata.
     *
     * @param logFile The log file to parse.
     * @return A map of topic names to their UUIDs.
     * @throws IOException If an I/O error occurs.
     */
    private static Map<String, UUID> parseClusterMetadataLog(File logFile) throws IOException {
        Map<String, UUID> topicMetadata = new HashMap<>();
        try (FileInputStream fis = new FileInputStream(logFile)) {
            byte[] buffer = new byte[(int) logFile.length()];
            fis.read(buffer);
            ByteBuffer bb = ByteBuffer.wrap(buffer);

            // Parse records from the log file
            while (bb.remaining() >= 4) { // Ensure there's enough data for the next record
                int recordLength = bb.getInt();
                if (recordLength <= 0 || bb.remaining() < recordLength) {
                    break; // Invalid record length or not enough data
                }

                byte[] recordBytes = new byte[recordLength];
                bb.get(recordBytes);
                ByteBuffer record = ByteBuffer.wrap(recordBytes);

                // Example: Extract topic name and topic ID (simplified)
                if (record.remaining() >= 2) {
                    short topicNameLength = record.getShort();
                    if (record.remaining() >= topicNameLength + 16) {
                        byte[] topicNameBytes = new byte[topicNameLength];
                        record.get(topicNameBytes);
                        String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);

                        long mostSigBits = record.getLong();
                        long leastSigBits = record.getLong();
                        UUID topicId = new UUID(mostSigBits, leastSigBits);

                        topicMetadata.put(topicName, topicId);
                    }
                }
            }
        }
        return topicMetadata;
    }

    /**
     * Sends a DescribeTopicPartitions response for the requested topic.
     *
     * @param inputBuf      The input buffer containing the request.
     * @param out          The output stream to write the response.
     * @param correlationId The correlation ID of the request.
     * @throws IOException If an I/O error occurs.
     */
    private static void sendDescribeTopicPartitionsResponse(ByteBuffer inputBuf, OutputStream out, int correlationId) throws IOException {
        int clientIdLength = inputBuf.getShort();
        byte[] clientId = new byte[clientIdLength];
        inputBuf.get(clientId);
        inputBuf.get(); // Ignore one byte
        int arrayLength = inputBuf.get();
        int topicNameLength = inputBuf.get();
        byte[] topicNameBytes = new byte[topicNameLength];
        inputBuf.get(topicNameBytes);
        String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);

        // Parse the __cluster_metadata log file to get topic metadata
        File logFile = new File(CLUSTER_METADATA_LOG_PATH);
        if (!logFile.exists()) {
            System.err.println("Cluster metadata log file not found.");
            sendErrorResponse(out, correlationId);
            return;
        }

        // Parse the log file to find the topic metadata
        Map<String, UUID> topicMetadata = parseClusterMetadataLog(logFile);
        UUID topicId = topicMetadata.get(topicName);
        if (topicId == null) {
            System.err.println("Topic not found in cluster metadata.");
            sendErrorResponse(out, correlationId);
            return;
        }

        // Construct the response
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(ByteBuffer.allocate(4).putInt(correlationId).array()); // Correlation ID
        bos.write(0); // No error
        bos.write(ByteBuffer.allocate(4).putInt(0).array()); // Throttle time (4 bytes)
        bos.write(1); // Number of topics (array length)
        bos.write(new byte[]{0, (byte) NO_ERROR_CODE}); // Error code
        bos.write(topicNameLength); // Topic name length
        bos.write(topicNameBytes); // Topic name
        bos.write(ByteBuffer.allocate(16)
                .putLong(topicId.getMostSignificantBits())
                .putLong(topicId.getLeastSignificantBits())
                .array()); // Topic ID
        bos.write(1); // Number of partitions
        bos.write(new byte[]{0, (byte) NO_ERROR_CODE}); // Partition error code
        bos.write(ByteBuffer.allocate(4).putInt(0).array()); // Partition index (0)
        bos.write(new byte[]{0, 0, 0, 0}); // Leader ID
        bos.write(new byte[]{0, 0, 0, 0}); // Leader epoch
        bos.write(new byte[]{0, 0, 0, 0}); // Replica nodes
        bos.write(new byte[]{0, 0, 0, 0}); // ISR nodes
        bos.write(new byte[]{0, 0, 0, 0}); // Offline replicas
        bos.write(0); // is_internal (0 = false)
        bos.write(0); // Tagged fields end byte

        // Write the response
        int size = bos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array()); // Message size
        out.write(bos.toByteArray()); // Payload
        out.flush();

        System.err.printf("Correlation ID: %d - Sent DescribeTopicPartitions response for topic %s%n", correlationId, topicName);
    }

    /**
     * Sends an error response.
     *
     * @param out          The output stream to write the response.
     * @param correlationId The correlation ID of the request.
     * @throws IOException If an I/O error occurs.
     */
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

    /**
     * Sends an APIVersions response.
     *
     * @param out          The output stream to write the response.
     * @param correlationId The correlation ID of the request.
     * @throws IOException If an I/O error occurs.
     */
    private static void sendAPIVersionsResponse(OutputStream out, int correlationId) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.err.println("CorrelationID: " + correlationId);
        bos.write(ByteBuffer.allocate(4).putInt(correlationId).array()); // Correlation ID
        bos.write(new byte[]{0, (byte) NO_ERROR_CODE}); // No error
        bos.write(3); // Number of API keys
        bos.write(new byte[]{0, (byte) API_VERSIONS_KEY}); // API key (API_VERSIONS_KEY)
        bos.write(new byte[]{0, (byte) SUPPORTED_API_VERSION_MIN}); // Min version
        bos.write(new byte[]{0, (byte) SUPPORTED_API_VERSION_MAX}); // Max version
        bos.write(0);
        bos.write(new byte[]{0, 75});
        bos.write(new byte[]{0, 0});
        bos.write(new byte[]{0, 0});
        bos.write(0);
        bos.write(new byte[]{0, 0, 0, 0}); // Throttle time
        bos.write(0); // Tagged fields end byte
        int size = bos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array()); // Message size
        out.write(bos.toByteArray()); // Payload
        out.flush();
        System.err.printf("Correlation ID: %d - Sent APIVersions response with no error.%n", correlationId);
    }

    /**
     * Handles client connections and processes requests.
     */
    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            System.err.println("Client connected: " + clientSocket.getInetAddress());
            try (DataInputStream di = new DataInputStream(clientSocket.getInputStream());
                 OutputStream out = clientSocket.getOutputStream()) {
                while (true) {
                    int dataLen = di.readInt();
                    if (dataLen <= 0) {
                        System.err.printf("Invalid message size received: %d%n", dataLen);
                        break;
                    }
                    System.err.printf("Data length: %d%n", dataLen);
                    byte[] tmp = new byte[dataLen];
                    int bytesRead = di.read(tmp);
                    if (bytesRead != dataLen) {
                        System.err.println("Incomplete message received.");
                        break;
                    }
                    ByteBuffer inputBuf = ByteBuffer.wrap(tmp);
                    short apiKey = inputBuf.getShort();
                    short apiVersion = inputBuf.getShort();
                    int correlationId = inputBuf.getInt();
                    System.err.printf("API Key: %d, API Version: %d, Correlation ID: %d%n", apiKey, apiVersion, correlationId);
                    if (apiKey == DESCRIBE_TOPIC_PARTITIONS_KEY && apiVersion == 0) {
                        sendDescribeTopicPartitionsResponse(inputBuf, out, correlationId);
                    } else if (apiKey == API_VERSIONS_KEY &&
                             apiVersion >= SUPPORTED_API_VERSION_MIN &&
                             apiVersion <= SUPPORTED_API_VERSION_MAX) {
                        sendAPIVersionsResponse(out, correlationId);
                    } else {
                        sendErrorResponse(out, correlationId);
                    }
                }
            } catch (IOException e) {
                System.err.println("IOException while handling client: " + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.err.println("IOException while closing client socket: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Main entry point for the server.
     */
    public static void main(String[] args) {
        System.err.println("Starting server on port " + PORT + "...");
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    Thread clientThread = new Thread(new ClientHandler(clientSocket));
                    clientThread.start();
                } catch (IOException e) {
                    System.err.println("IOException while accepting client: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("IOException while starting server: " + e.getMessage());
        }
    }
}