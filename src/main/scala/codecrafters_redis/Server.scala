package codecrafters_redis

import scopt.OParser
import scala.util.Random

import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.io.{BufferedReader, InputStreamReader, OutputStream, File}
import java.util.concurrent.Executors
import java.util.Base64
import javax.xml.crypto.Data
import codecrafters_redis.ClusterNode.managerPort

case class Config(
    dir: String = "",
    dbFileName: String = "",
    portNumber: String = "6379",
    var replicaOf: String = ""
) {

  def getValue(key: String): Option[String] = {
    key match {
      case "dir"        => Some(this.dir)
      case "dbfilename" => Some(this.dbFileName)
      case "port"       => Some(this.portNumber)
      case "replicaof"  => Some(this.replicaOf)
      case other        => None
    }
  }

}

class RequestThread(clientSocket: Socket, storage: DataStorage, config: Config) extends Thread {

  def execute(
      input: List[String],
      command: Command,
      storage: DataStorage,
      config: Config,
      protocol: Protocol,
      clientSocket: Socket
  ): Option[RespReply] = {
    val writeCommands = List[String]("SET", "DEL")

    if (Server.getRole() == "manager" & writeCommands.contains(input(0).toUpperCase())) {
      ClusterManager.routeCommandToNode(input, protocol)
      writeToStream(RespSimpleString("OK"), protocol, clientSocket.getOutputStream)
      None
    } else {
      Some(command.execute(input, storage, config, protocol, clientSocket))
    }
  }

  def writeToStream(input: RespReply, protocol: Protocol, outputStream: OutputStream): Unit = {
    val output = protocol.write(input)
    outputStream.write(output)
    outputStream.flush()
  }

  override def run(): Unit = {
    try {
      val inputReader = new BufferedReader(
        new InputStreamReader(clientSocket.getInputStream)
      )
      val outputStream = clientSocket.getOutputStream

      val protocol = new Protocol()
      val command = new Command()

      while (true) {

        try {
          val input = protocol.read(inputReader)
          val result = execute(input, command, storage, config, protocol, clientSocket)
          // val result = command.execute(input, storage, config, protocol, clientSocket)
          result match {
            case Some(value) => writeToStream(value, protocol, outputStream)
            case None        => println("Forwarded command to node(s)")
          }

          if (input(0).toUpperCase() == "PSYNC") {
            ClusterManager.sendSnapshot(protocol)
          }
        } catch {
          case e: UnsupportedOperationException =>
            println(s"Unrecognized command: $e")
          case e: RuntimeException =>
            println("Client disconnected, closing thread")
            clientSocket.close()
            return
        }
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

  private val replID: String = setReplID()
  private var replOffset: Int = 0
  private var role: String = "standalone"

  def setReplID(): String = {
    val arr = new Array[Byte](20)
    Random.nextBytes(arr)

    arr.iterator.map(x => String.format("%02x", Byte.box(x))).mkString("")
  }

  def getReplID(): String = {
    this.replID
  }

  def getReplOffset(): Int = {
    this.replOffset
  }

  def setRole(role: String): Unit = {
    this.role = role
  }

  def getRole(): String = {
    this.role
  }

  def configureClusterManager(nodeSocket: Socket): Unit = {
    ClusterManager.setNodeSocket(nodeSocket)
    ClusterManager.setNodePort()
    ClusterManager.setNodeHost()
    ClusterManager.setNodeInputStream()
    ClusterManager.setNodeOutputStream()
    setRole("manager")
  }

  def configureClusterNode(config: Config): Socket = {
    val managerHost = config.replicaOf.split(" ")(0)
    val managerPort = config.replicaOf.split(" ")(1).toInt

    val protocol = new Protocol()

    ClusterNode.setManagerPort(managerPort)
    ClusterNode.setManagerHost(managerHost)
    ClusterNode.setManagerSocket()
    ClusterNode.setManagerInputStream()
    ClusterNode.setManagerOutputStream()
    ClusterNode.register(protocol, config.portNumber)
    setRole("node")
    config.replicaOf = ""

    ClusterNode.managerSocket match {
      case Some(socket) => socket
      case None         =>
        throw new RuntimeException("Manager socket is not defined, cannot register as node")
    }
  }

  def main(args: Array[String]): Unit = {

    val cliParser = new scopt.OptionParser[Config]("scopt") {
      head("scopt", "4.x")

      opt[String]("dir")
        .optional()
        .action((x, c) => c.copy(dir = x))
        .text("dir is the dbfile directory name")

      opt[String]("dbfilename")
        .optional()
        .action((x, c) => c.copy(dbFileName = x))
        .text("dbfilename is the name of the stored dbfile")

      opt[String]("port")
        .optional()
        .action((x, c) => c.copy(portNumber = x))
        .text("Specify a custom port number, default is 6379")

      opt[String]("replicaof")
        .optional()
        .action((x, c) => c.copy(replicaOf = x))
        .validate(x =>
          if (x.split(" ").length == 2) success
          else failure("Should follow the '<MASTER_HOST> <MASTER_PORT>' pattern")
        )
        .text("Specify whether the instance is a replica, pattern is '<MASTER_HOST> <MASTER_PORT>'")
    }

    val config = cliParser.parse(args, Config()) match {
      case Some(conf) =>
        println(s"Using dir: ${conf.dir}, dbfilename: ${conf.dbFileName}, port: ${conf.portNumber}")
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
      val clientSocket = config.replicaOf match {
        case ""        => serverSocket.accept()
        case _: String => configureClusterNode(config)
      }
      println("New client connected!")
      threadPool.execute(new RequestThread(clientSocket, storage, config))
    }
  }
}
