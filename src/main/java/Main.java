import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

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

                    // Read the request (though we don't process it yet)
                    byte[] requestBuffer = new byte[1024];
                    int bytesRead = inputStream.read(requestBuffer);
                    System.err.println("Received request of size: " + bytesRead + " bytes");

                    // Hardcoded response values
                    int messageSize = 4;  // Placeholder
                    int correlationId = 7; // Hardcoded

                    // Construct the response
                    ByteBuffer buffer = ByteBuffer.allocate(8); // 4 bytes for message_size + 4 bytes for correlation_id
                    buffer.putInt(messageSize);
                    buffer.putInt(correlationId);

                    // Send the response
                    outputStream.write(buffer.array());
                    outputStream.flush();
                    System.err.println("Sent response with correlation_id = 7");

                } catch (IOException e) {
                    System.err.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
    }
}
