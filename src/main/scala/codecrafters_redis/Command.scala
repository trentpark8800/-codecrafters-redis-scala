package codecrafters_redis

trait RespReply

case class RespSimpleString(value: String) extends RespReply
case class RespError(message: String) extends RespReply
case class RespInteger(value: Integer) extends RespReply
case class RespBulkString(value: String) extends RespReply
case class RespArray(value: List[RespReply]) extends RespReply
case object RespNull extends RespReply

class Command {

  def commandCommand(): RespReply = {
    RespSimpleString("OK")
  }

  def pingCommand(): RespReply = {
    RespSimpleString("PONG")
  }

  def echoCommand(input: List[String]): RespReply = {
    if (input.length > 1) {
      throw new RuntimeException(s"ECHO command expects 1 argument but received ${input.length}.")
    }

    RespBulkString(input.head)
  }

  def setCommand(input: List[String], storage: DataStorage): RespReply = {
    if (input.length > 4) {
      throw new RuntimeException(
        s"SET command expects up to 4 arguments but received ${input.length}."
      )
    }

    val key = input.head
    val value = input(1)

    if (input.length == 4) {
      val secondCommand = input(2).toUpperCase()
      val secondArg = input(3)
      var ttl = Some(secondArg.toLong)

      storage.setValue(key, value, ttl)
    } else {
      storage.setValue(key, value, None)
    }

    RespSimpleString("OK")
  }

  def getCommand(input: List[String], storage: DataStorage): RespReply = {
    if (input.length != 1) {
      throw new RuntimeException(
        s"GET command expects exactly 1 argument but received ${input.length}."
      )
    }

    val key = input.head

    val value = storage.getValue(key)

    value match {
      case Some(v) => RespBulkString(v)
      case None    => RespNull
    }
  }

  def keysCommand(input: List[String], storage: DataStorage): RespReply = {
    if (input.length != 1) {
      throw new RuntimeException(
        s"KEYS command expects exactly 1 argument but received ${input.length}."
      )
    }

    val subCommand = input.head

    subCommand match {
      case "*" => RespArray(storage.getAllKeys().map(x => RespBulkString(x)).toList)
    }
  }

  def configCommand(input: List[String], config: Config): RespReply = {

    if (input.length != 2) {
      throw new RuntimeException(s"CONFIG command requires a sub-command and argument.")
    }

    val subCommand = input.head.toUpperCase()
    val subarg = input(1).toLowerCase()

    subCommand match {
      case "GET" =>
        config.getValue(subarg) match {
          case Some(value) => RespArray(List(RespBulkString(subarg), RespBulkString(value)))
          case None        => RespNull
        }
    }
  }

  def infoCommand(input: List[String], config: Config): RespReply = {

    if (input.length != 1) {
      RespBulkString("default info reply")
    }

    val subCommand = input.head.toUpperCase()

    subCommand match {
      case "REPLICATION" => if (config.replicaOf.isEmpty()) RespBulkString("role:master") else RespBulkString("role:slave")
    }
  }

  def execute(input: List[String], storage: DataStorage, config: Config): RespReply = {
    if (input.isEmpty) throw new Exception("No command received")

    val command =
      input.headOption.getOrElse(throw new Exception("No command received")).toUpperCase()
    val commandInput = input.slice(1, input.length)

    command match {
      case "COMMAND" => commandCommand()
      case "PING"    => pingCommand()
      case "ECHO"    => echoCommand(commandInput)
      case "SET"     => setCommand(commandInput, storage)
      case "GET"     => getCommand(commandInput, storage)
      case "KEYS"    => keysCommand(commandInput, storage)
      case "CONFIG"  => configCommand(commandInput, config)
      case "INFO"    => infoCommand(commandInput, config)
      case other     => throw new Exception(s"Command $other not recognised.")
    }
  }
}
