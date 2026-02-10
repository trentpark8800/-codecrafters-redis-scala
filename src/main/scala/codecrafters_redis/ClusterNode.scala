package codecrafters_redis

import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.io.{BufferedReader, InputStreamReader, OutputStream, File}

object ClusterNode {

  var managerPort = 0
  var managerHost = ""
  var managerSocket: Option[Socket] = None
  var managerOutputStream: Option[OutputStream] = None
  var managerInputStream: Option[BufferedReader] = None

  def setManagerPort(managerPort: Int): Unit = {
    this.managerPort = managerPort
  }

  def setManagerHost(host: String): Unit = {
    this.managerHost = host
  }

  def setManagerSocket(): Unit = {
    if (this.managerPort == 0) {
      throw new RuntimeException("Manager port is blank and needs to be set")
    }

    this.managerSocket = Some(new Socket(this.managerHost, this.managerPort))
  }

  def setManagerOutputStream(): Unit = {
    this.managerOutputStream = this.managerSocket match {
      case Some(socket) => Some(socket.getOutputStream)
      case None         => throw new RuntimeException("Socket is not defined")
    }
  }

  def setManagerInputStream(): Unit = {
    this.managerInputStream = this.managerSocket match {
      case Some(socket) => Some(new BufferedReader(new InputStreamReader(socket.getInputStream)))
      case None         => throw new RuntimeException("Socket is not defined")
    }
  }

  private def writeToManager(input: Array[Byte]): Unit = {

    this.managerOutputStream match {
      case Some(theOutputStream) => theOutputStream.write(input)
      case None => throw new RuntimeException("Manager output stream is not defined")
    }

    this.managerOutputStream match {
      case Some(theOutputStream) => theOutputStream.flush()
      case None => throw new RuntimeException("Manager output stream is not defined")
    }

  }

  private def readFromManager(protocol: Protocol): List[String] = {

    this.managerInputStream match {
      case Some(theInputStream) => protocol.read(theInputStream)
      case None                 => throw new RuntimeException("Manager input stream is not defined")
    }
  }

  // Handshake and register manager
  def register(protocol: Protocol, listeningPort: String): Unit = {

    val outputPing = protocol.write(RespArray(List(RespBulkString("PING"))))
    writeToManager(outputPing)

    val pingResponse = readFromManager(protocol)

    if (pingResponse(0) == "PONG") {

      var replConf = protocol.write(
        RespArray(
          List(
            RespBulkString("REPLCONF"),
            RespBulkString("listening-port"),
            RespBulkString(listeningPort)
          )
        )
      )

      writeToManager(replConf)

      replConf = protocol.write(
        RespArray(
          List(
            RespBulkString("REPLCONF"),
            RespBulkString("capa"),
            RespBulkString("psync2")
          )
        )
      )

      writeToManager(replConf)

      val replConfResponse = readFromManager(protocol)

      if (replConfResponse(0) != "") {

        val psyncArr = protocol.write(
          RespArray(
            List(
              RespBulkString("PSYNC"),
              RespBulkString("?"),
              RespBulkString("-1")
            )
          )
        )

        writeToManager(psyncArr)
      }

    } else {
      throw new RuntimeException("Handshake with manager failed!")
    }
    println(s"Handshake successful!")

  }

}
