package codecrafters_redis

class Command {

  def echoCommand(input: String): String = {
    input
  }

  def pingCommand(): String = {
    "PONG"
  }

  def execute(input: List[String]): String = {
    if (input.isEmpty) throw new Exception("No command received")

    val command = input.head.toUpperCase
    val commandInput = input.tail.headOption.getOrElse("")

    command match {
      case "PING" => pingCommand()
      case "ECHO" => echoCommand(commandInput)
      case other  => throw new Exception(s"Command $other not recognised.")
    }
  }
}
