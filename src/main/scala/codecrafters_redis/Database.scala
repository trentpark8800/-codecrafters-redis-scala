package codecrafters_redis

import java.io.{File, PrintWriter, FileWriter}
import scala.io.Source

trait Database {
    def create(): Unit
    def read(): Unit
    def update(): Unit
    def delete(): Unit
}

object RedisDatabase extends Database {
    val fileName = "dump.rdb"

    def create(): Unit = {
        val file = new File(fileName)
        if (!file.exists()) {
            val writer = new PrintWriter(file)
            writer.println("# Redis RDB file - simple demo format")
            writer.close()
            println(s"Created $fileName")
        } else {
            println(s"$fileName already exists.")
        }
    }

    def read(): Unit = {
        val file = new File(fileName)
        if (file.exists()) {
            println(s"Contents of $fileName:")
            Source.fromFile(file).getLines().foreach(println)
        } else {
            println(s"$fileName does not exist.")
        }
    }

    def update(): Unit = {
        val file = new File(fileName)
        if (file.exists()) {
            val fw = new FileWriter(file, true)
            fw.write(s"key:sampleKey value:sampleValue\n")
            fw.close()
            println(s"Appended sample key-value to $fileName")
        } else {
            println(s"$fileName does not exist. Cannot update.")
        }
    }

    def delete(): Unit = {
        val file = new File(fileName)
        if (file.exists()) {
            file.delete()
            println(s"Deleted $fileName")
        } else {
            println(s"$fileName does not exist.")
        }
    }
}
