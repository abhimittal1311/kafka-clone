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

            // Wait for connection from client
            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     DataInputStream in = new DataInputStream(clientSocket.getInputStream());
                     OutputStream out = clientSocket.getOutputStream()) {

                    System.out.println("Client connected.");

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
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static void handleRequest(DataInputStream inputStream, OutputStream outputStream) throws IOException {
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
        byteArrayStream.write(getErrorCode(requestApiVersion)); // 2 bytes

        //   .num_api_keys
        byteArrayStream.write(new byte[]{0, 1}); // Array length (2 bytes, standard INT16)

        //   .api_key
        byteArrayStream.write(new byte[]{0, 18}); // API key 18 (API_VERSIONS)

        //   .min_version
        byteArrayStream.write(new byte[]{0, 0}); // Min version (2 bytes)

        //   .max_version
        byteArrayStream.write(new byte[]{0, 4}); // Max version (2 bytes)

        //   .throttle_time_ms
        byteArrayStream.write(new byte[]{0, 0, 0, 0}); // Throttle time (4 bytes)

        //   .TAG_BUFFER
        byteArrayStream.write(new byte[]{0}); // Tagged fields (1 byte)

        // Write the response message
        byte[] responseBytes = byteArrayStream.toByteArray();
        outputStream.write(ByteBuffer.allocate(4).putInt(responseBytes.length).array()); // Message size (4 bytes)
        outputStream.write(responseBytes); // Response body
        outputStream.flush();

        System.out.println("Response sent: " + Arrays.toString(responseBytes));
    }

    // Returns the error code as a 2-byte array
    private static byte[] getErrorCode(short requestApiVersion) {
        if (requestApiVersion < 0 || requestApiVersion > 4) {
            return new byte[]{0, 35}; // Error code 35 (UNSUPPORTED_VERSION)
        } else {
            return new byte[]{0, 0}; // No error
        }
    }
}