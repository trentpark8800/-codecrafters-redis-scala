package codecrafters_redis

import scopt.OParser

import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.io.{BufferedReader, InputStreamReader, OutputStream, File}
import java.util.concurrent.Executors
import javax.xml.crypto.Data

case class Config(
    dir: String = "",
    dbFileName: String = "",
    portNumber: String = "6379"
) {

  def getValue(key: String): Option[String] = {
    key match {
      case "dir"        => Some(this.dir)
      case "dbfilename" => Some(this.dbFileName)
      case "port"       => Some(this.portNumber)
      case other        => None
    }
  }

}

class RequestThread(clientSocket: Socket, storage: DataStorage, config: Config) extends Thread {
  override def run(): Unit = {
    try {
      while (true) {
        val inputReader = new BufferedReader(
          new InputStreamReader(clientSocket.getInputStream)
        )
        val outputStream = clientSocket.getOutputStream

        val protocol = new Protocol()
        val command = new Command()

        val input = protocol.read(inputReader)
        val result = command.execute(input, storage, config)
        val output = protocol.write(result)

        outputStream.write(output)
        outputStream.flush()
      }
    } catch {
      case e: Exception =>
        println(s"Error handling client: ${e.getMessage}")
    } finally {
      clientSocket.close()
    }
  }
}

object Server {
  def main(args: Array[String]): Unit = {

    val cliParser = new scopt.OptionParser[Config]("scopt") {
      head("scopt", "4.x")

      opt[String]("dir")
        .action((x, c) => c.copy(dir = x))
        .text("dir is the dbfile directory name")

      opt[String]("dbfilename")
        .action((x, c) => c.copy(dbFileName = x))
        .text("dbfilename is the name of the stored dbfile")

      opt[String]("port")
        .action((x, c) => c.copy(portNumber = x))
        .text("Specify a custom port number, default is 6379")
    }

    val config = cliParser.parse(args, Config()) match {
      case Some(conf) =>
        println(s"Using dir: ${conf.dir}, dbfilename: ${conf.dbFileName}")
        conf
      case None =>
        println("No command line arguments specified")
        null
    }

    println(s"BYO redis server starting on localhost:${config.portNumber}...")

    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress("localhost", config.portNumber.toInt))

    println("BYO redis server successfully started!")

    val storage = InMemoryDataStorage

    println("Storage successfully instantiated.")

    if (config.dbFileName != "") {
      val rdbFileParser = new RdbParser()
      try {
        rdbFileParser.parse(s"${config.dir}/${config.dbFileName}")
        storage.cache = rdbFileParser.tempCache
        storage.expiryCache = rdbFileParser.tempExpiryCache
      } catch {
        case e: java.io.FileNotFoundException =>
          println(
            s"The specified file path ${config.dir}/${config.dbFileName} does not exist. Continuing..."
          )
      }
    }

    val threadPool = Executors.newFixedThreadPool(8)

    while (true) {
      val clientSocket = serverSocket.accept()
      println("New client connected!")
      threadPool.execute(new RequestThread(clientSocket, storage, config))
    }
  }
}
