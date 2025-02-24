package codecrafters_redis

import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.io.{BufferedReader, InputStreamReader, OutputStream}

class RequestThread(clientSocket: Socket) extends Thread {
  override def run(): Unit = {
    try {
      val inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))
      val outputStream = clientSocket.getOutputStream

      var requestLine: String = null
      while ({ requestLine = inputReader.readLine(); requestLine != null }) {
        println(s"Received request: $requestLine")

        if (requestLine.trim.toUpperCase == "PING") {
          outputStream.write("+PONG\r\n".getBytes)
          outputStream.flush()
        }
      }
    } catch {
      case e: Exception => println(s"Error handling client: ${e.getMessage}")
    } finally {
      clientSocket.close()
    }
  }
}

object Server {
  def main(args: Array[String]): Unit = {
    println("Logs from your program will appear here!")

    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress("localhost", 6379))

    while (true) {
      val clientSocket = serverSocket.accept() // Accept new client
      println("New client connected!")

      val requestThread = new RequestThread(clientSocket)
      requestThread.start() // Start a new thread for this client
    }
  }
}
