package codecrafters_redis

import java.net.{InetSocketAddress, ServerSocket}
import java.io.{BufferedReader, InputStreamReader, OutputStream}

object Server {
  def main(args: Array[String]): Unit = {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println("Logs from your program will appear here!")

    // Uncomment this to pass the first stage
    
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress("localhost", 6379))

    while (true) {
      val clientSocket = serverSocket.accept() // wait for clients
      val inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))
      val outputStream = clientSocket.getOutputStream

      var requestLine: String = null
      while ({ requestLine = inputReader.readLine(); requestLine != null }) {
        outputStream.write("+PONG\r\n".getBytes)
      }
    }
    
  }
}
