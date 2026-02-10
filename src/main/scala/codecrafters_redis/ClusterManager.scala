package codecrafters_redis

import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.io.{BufferedReader, InputStreamReader, OutputStream, File}
import java.util.Base64

object ClusterManager {

  var nodePort = 0
  var nodeHost = ""
  var nodeSocket: Option[Socket] = None
  var nodeOutputStream: Option[OutputStream] = None
  var nodeInputStream: Option[BufferedReader] = None

  def setNodeSocket(nodeSocket: Socket): Unit = {
    this.nodeSocket = Some(nodeSocket)
  }

  def setNodeOutputStream(): Unit = {
    this.nodeOutputStream = this.nodeSocket match {
      case Some(socket) => Some(socket.getOutputStream)
      case None         => throw new RuntimeException("Socket is not defined")
    }
  }

  def setNodeInputStream(): Unit = {
    this.nodeInputStream = this.nodeSocket match {
      case Some(socket) => Some(new BufferedReader(new InputStreamReader(socket.getInputStream)))
      case None         => throw new RuntimeException("Socket is not defined")
    }
  }

  def setNodePort(): Unit = {
    this.nodePort = this.nodeSocket match {
      case Some(socket) => socket.getPort()
      case None => throw new RuntimeException("Socket is not defined")
    }
  }

  def setNodeHost(): Unit = {
    this.nodeHost = this.nodeSocket match {
      case Some(socket) => socket.getInetAddress().getHostAddress()
      case None => throw new RuntimeException("Socket is not defined")
    }
  }

  private def writeToNode(input: Array[Byte]): Unit = {

    this.nodeOutputStream match {
      case Some(theOutputStream) => theOutputStream.write(input)
      case None                  => throw new RuntimeException("Node output stream is not defined")
    }

    this.nodeOutputStream match {
      case Some(theOutputStream) => theOutputStream.flush()
      case None                  => throw new RuntimeException("Node output stream is not defined")
    }

  }

  def routeCommandToNode(input: List[String], protocol: Protocol): Unit = {

    val respBulkStrings: List[RespReply] = input.map(x => RespBulkString(x.toString)).toList

    val respInput = RespArray(respBulkStrings)

    val encodedInput = protocol.write(respInput)

    writeToNode(encodedInput)
  }

  def sendSnapshot(protocol: Protocol): Unit = {

    val b64EncodedCacheSnapshot =
      "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

    val cacheSnapshot = RespSnapshot(Base64.getDecoder().decode(b64EncodedCacheSnapshot))

    val encodedSnapshotLength = protocol.write(cacheSnapshot)

    writeToNode(encodedSnapshotLength)
    writeToNode(cacheSnapshot.value)

  }

}
