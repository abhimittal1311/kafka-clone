import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import log.RecordBatch;
import requests.Request;
import responses.Response;
public class Main {
  private static final ExecutorService executorService =
      Executors.newFixedThreadPool(8);
  private static final Path logPath = Path.of(
      "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
  private static final List<RecordBatch> batches = getRecordBatches();
  ;
  public static void main(String[] args) {
    int port = 9092;
    System.err.println("Starting server on port " + port + "...");
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      // Since the tester restarts your program quite often, setting
      // SO_REUSEADDR ensures that we don't run into 'Address already in use'
      // errors
      serverSocket.setReuseAddress(true);
      // noinspection InfiniteLoopStatement
      while (true) {
        // Wait for connection from client.
        Socket clientSocket = serverSocket.accept();
        executorService.submit(() -> handleClient(clientSocket));
      }
    } catch (IOException e) {
      System.err.println("IOException: " + Arrays.toString(e.getStackTrace()));
    } finally {
      executorService.shutdown();
    }
  }
  private static void handleClient(Socket clientSocket) {
    try (DataInputStream dis =
             new DataInputStream(clientSocket.getInputStream());
         OutputStream os = clientSocket.getOutputStream()) {
      // noinspection InfiniteLoopStatement
      while (true) {
        if (dis.available() > 0) {
          int len = dis.readInt();
          byte[] requestBytes = new byte[len];
          dis.readFully(requestBytes);
          ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
          Request<?> request = Request.fromByteBuffer(requestBuffer);
          Response response = new Response(request, batches);
          os.write(response.toBytes());
          os.flush();
        }
      }
    } catch (IOException e) {
      System.err.println(Arrays.toString(e.getStackTrace()));
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.err.println("IOException: " +
                           Arrays.toString(e.getStackTrace()));
      }
    }
  }
  private static List<RecordBatch> getRecordBatches() {
    List<RecordBatch> batches = new ArrayList<>();
    try (FileChannel fileChannel =
             FileChannel.open(logPath, StandardOpenOption.READ)) {
      ByteBuffer data =
          fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
      while (data.hasRemaining()) {
        RecordBatch batch = RecordBatch.fromByteBuffer(data);
        batches.add(batch);
      }
    } catch (IOException ioNo) {
      System.err.println("IOException: " +
                         Arrays.toString(ioNo.getStackTrace()));
    }
    return batches;
  }
}