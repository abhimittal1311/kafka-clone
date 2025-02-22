import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        System.out.println("fafka server started");

        int port = 9092;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
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
}

class ClientHandler implements Runnable {
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
                    handleRequest(in, out);
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

    private void handleRequest(DataInputStream inputStream, OutputStream outputStream) throws IOException {
        // Read the request message
        int incomingMessageSize = inputStream.readInt();
        byte[] requestApiKeyBytes = inputStream.readNBytes(2);
        short requestApiVersion = inputStream.readShort();
        byte[] correlationIdBytes = inputStream.readNBytes(4);
        byte[] remainingBytes = new byte[incomingMessageSize - 8];
        inputStream.readFully(remainingBytes);

        // Build the response message
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();

        // .ResponseHeader
        //   .correlation_id
        byteArrayStream.write(correlationIdBytes);

        // .ResponseBody
        //   .error_code
        byteArrayStream.write(getErrorCode(requestApiVersion)); // 1 or 2 bytes

        //   .num_api_keys
        byteArrayStream.write(new byte[]{0, 2}); // Array length (2 bytes, non-standard)

        // Entry for APIVersions (API key 18)
        byteArrayStream.write(new byte[]{0, 18}); // API key 18 (APIVersions)
        byteArrayStream.write(new byte[]{0, 0}); // Min version (2 bytes)
        byteArrayStream.write(new byte[]{0, 4}); // Max version (2 bytes)

        // Entry for DescribeTopicPartitions (API key 75)
        byteArrayStream.write(new byte[]{0, 75}); // API key 75 (DescribeTopicPartitions)
        byteArrayStream.write(new byte[]{0, 0}); // Min version (2 bytes)
        byteArrayStream.write(new byte[]{0, 0}); // Max version (2 bytes)

        //   .TAG_BUFFER
        byteArrayStream.write(new byte[]{0});

        //   .throttle_time_ms
        byteArrayStream.write(new byte[]{0, 0, 0, 0});

        //   .TAG_BUFFER
        byteArrayStream.write(new byte[]{0});

        // Write the response message
        byte[] responseBytes = byteArrayStream.toByteArray();
        outputStream.write(ByteBuffer.allocate(4).putInt(responseBytes.length).array()); // Message size (4 bytes)
        outputStream.write(responseBytes); // Response body
        outputStream.flush();

        System.out.println("Response sent to client: " + Arrays.toString(responseBytes));
    }

    // Returns the error code as a 1 or 2-byte array
    private byte[] getErrorCode(short requestApiVersion) {
        if (requestApiVersion < 0 || requestApiVersion > 4) {
            return ByteBuffer.allocate(2).putShort((short) 35).array(); // Error code 35 (UNSUPPORTED_VERSION)
        } else {
            return new byte[]{0}; // No error (1 byte)
        }
    }
}