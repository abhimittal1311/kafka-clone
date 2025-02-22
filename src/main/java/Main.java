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

        // Read the __cluster_metadata log file to get topic metadata
        File logFile = new File(CLUSTER_METADATA_LOG_PATH);
        if (!logFile.exists()) {
            System.err.println("Cluster metadata log file not found.");
            sendErrorResponse(out, correlationId);
            return;
        }

        // Parse the log file to find the topic metadata
        // This is a simplified example; in a real implementation, you would need to parse the log file properly
        String topicId = UUID.randomUUID().toString(); // Placeholder for topic UUID
        int partitionId = 0; // Placeholder for partition ID

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(ByteBuffer.allocate(4).putInt(correlationId).array()); // Correlation ID
        bos.write(0); // No error
        bos.write(ByteBuffer.allocate(4).putInt(0).array()); // Throttle time
        bos.write(arrayLength); // Array length
        bos.write(new byte[]{0, (byte) NO_ERROR_CODE}); // Error code
        bos.write(topicNameLength); // Topic name length
        bos.write(topicNameBytes); // Topic name
        bos.write(ByteBuffer.allocate(16).putLong(UUID.randomUUID().getMostSignificantBits()).putLong(UUID.randomUUID().getLeastSignificantBits()).array()); // Topic ID
        bos.write(1); // Number of partitions
        bos.write(new byte[]{0, (byte) NO_ERROR_CODE}); // Partition error code
        bos.write(ByteBuffer.allocate(4).putInt(partitionId).array()); // Partition index
        bos.write(new byte[]{0, 0, 0, 0}); // Leader ID
        bos.write(new byte[]{0, 0, 0, 0}); // Leader epoch
        bos.write(new byte[]{0, 0, 0, 0}); // Replica nodes
        bos.write(new byte[]{0, 0, 0, 0}); // ISR nodes
        bos.write(new byte[]{0, 0, 0, 0}); // Offline replicas
        bos.write(0); // Tagged fields end byte

        int size = bos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array()); // Message size
        out.write(bos.toByteArray()); // Payload
        out.flush();

        System.err.printf("Correlation ID: %d - Sent DescribeTopicPartitions response for topic %s%n", correlationId, topicName);
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
                    } else if (apiKey == API_VERSIONS_KEY && apiVersion >= SUPPORTED_API_VERSION_MIN && apiVersion <= SUPPORTED_API_VERSION_MAX) {
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