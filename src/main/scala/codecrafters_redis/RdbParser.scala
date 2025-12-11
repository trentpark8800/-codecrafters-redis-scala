package codecrafters_redis

import java.io.FileInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

trait Parser {
    def parse(filePath: String): Unit
}

class RdbParser extends Parser {

    private val REDIS_RDB_OPCODE_META = 250
    private val REDIS_RDB_OPCODE_RESIZEDB = 251
    private val REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
    private val REDIS_RDB_OPCODE_EXPIRETIME = 253
    private val REDIS_RDB_OPCODE_SELECTDB = 254
    private val REDIS_RDB_OPCODE_EOF = 255

    private val REDIS_RDB_TYPE_STRING = 0
    private val REDIS_RDB_TYPE_LIST = 1
    private val REDIS_RDB_TYPE_SET = 2
    private val REDIS_RDB_TYPE_ZSET = 3
    private val REDIS_RDB_TYPE_HASH = 4
    private val REDIS_RDB_TYPE_ZSET_2 = 5
    private val REDIS_RDB_TYPE_MODULE = 6
    private val REDIS_RDB_TYPE_MODULE_2 = 7
    private val REDIS_RDB_TYPE_HASH_ZIPMAP = 9
    private val REDIS_RDB_TYPE_LIST_ZIPLIST = 10
    private val REDIS_RDB_TYPE_SET_INTSET = 11
    private val REDIS_RDB_TYPE_ZSET_ZIPLIST = 12
    private val REDIS_RDB_TYPE_HASH_ZIPLIST = 13
    private val REDIS_RDB_TYPE_LIST_QUICKLIST = 14
    private val REDIS_RDB_TYPE_STREAM_LISTPACKS = 15

    private val REDIS_RDB_ENC_INT8 = 0
    private val REDIS_RDB_ENC_INT16 = 1
    private val REDIS_RDB_ENC_INT32 = 2
    private val REDIS_RDB_ENC_LZF = 3

    var magicString = ""
    var redisVersion = ""
    var metaAttribute = ""
    var metaValue = ""
    private val CHARSET = "ASCII"
    val tempCache = scala.collection.mutable.Map[String, String]()
    val tempExpiryCache = scala.collection.mutable.Map[String, Long]()

    // Reads an encoded length as per Redis RDB format.
    // Handles 6-bit, 14-bit, and 32-bit encodings.
    private def readLengthWithEncoding(iter: Iterator[Int]): (Int, Boolean) = {
        val firstByte = iter.next()
        val typeBits = (firstByte & 0xC0) >> 6 // top 2 bits

        // println(s"typeBits value is $typeBits")

        typeBits match {
            case 0 => // 6 bit length, no special encoding
                (firstByte & 0x3F, false)
            case 1 => // 14 bit length, no special encoding
                val secondByte = iter.next()
                (((firstByte & 0x3F) << 8) | secondByte, false)
            case 3 => // 6 bit length, encoded of a type that is not string
                (firstByte & 0x3F, true)
            case _ =>
                throw new RuntimeException("Special encoding for length not supported")
        }
    }

    private def readBytesOfLength(iter: Iterator[Int], length: Int): Array[Byte] = {
        if (length < 1) throw new RuntimeException(s"Bytes length must be > 0, got $length instead")

        (1 to length).map(_ => if (iter.hasNext) iter.next().toByte else 0.toByte).toArray
    }

    private def readMagicString(fileIter: Iterator[Int]): Unit = {

        val bytes = (1 to 5).map(_ => if (fileIter.hasNext) fileIter.next().toByte else 0.toByte).toArray

        magicString = new String(bytes, CHARSET)
    }

    private def readVersion(fileIter: Iterator[Int]): Unit = {

        val bytes = (1 to 4).map(_ => if (fileIter.hasNext) fileIter.next().toByte else 0.toByte).toArray

        redisVersion = new String(bytes, CHARSET)
    }

    private def readEncodedValue(fileIter: Iterator[Int], lengthEncoding: Int): Array[Byte] = {
        
        lengthEncoding match {
            case REDIS_RDB_ENC_INT8 => Array(fileIter.next().toByte)
            case REDIS_RDB_ENC_INT16 => (1 to 2).map(_ => if (fileIter.hasNext) fileIter.next().toByte else 0.toByte).toArray
            case REDIS_RDB_ENC_INT32 => (1 to 4).map(_ => if (fileIter.hasNext) fileIter.next().toByte else 0.toByte).toArray
            // case REDIS_RDB_ENC_LZF => 
        }
    }

    private def readMetadata(fileIter: Iterator[Int]): Unit = {

        val (attributeLength, _) = readLengthWithEncoding(fileIter)
        // println(s"attributeLength is $attributeLength")
        val attributeBytes = (1 to attributeLength).map(_ => if (fileIter.hasNext) fileIter.next().toByte else 0.toByte).toArray

        val (valueLength, encoded) = readLengthWithEncoding(fileIter)
        // println(s"valueLength is $valueLength")

        var valueBytes = new Array[Byte](0)

        if (encoded == true) {
            valueBytes = readEncodedValue(fileIter, valueLength)
        } else {
            valueBytes = (1 to valueLength).map(_ => if (fileIter.hasNext) fileIter.next().toByte else 0.toByte).toArray
        }
        metaAttribute = new String(attributeBytes, CHARSET)
        metaValue = new String(valueBytes, CHARSET)

        // println(s"metaAttribute is $metaAttribute, metaValue is $metaValue")
    }

    private def readString(fileIter: Iterator[Int]): String = {

        val (byteLength, encoded) = readLengthWithEncoding(fileIter)

        val valueBytes = readBytesOfLength(fileIter, byteLength)

        new String(valueBytes, CHARSET)

    }

    private def readFourByteLittleEndian(fileIter: Iterator[Int]): Long = {

        val bytes = (1 to 4).map(_ => if (fileIter.hasNext) fileIter.next().toByte else 0.toByte).toArray

        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt().toLong
    }

    private def readEightByteLittleEndian(fileIter: Iterator[Int]): Long = {

        val bytes = (1 to 8).map(_ => if (fileIter.hasNext) fileIter.next().toByte else 0.toByte).toArray

        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt().toLong
    }

    private def processDatabase(fileIter: Iterator[Int]): Unit = {

        val databaseIndex = fileIter.next()
        val databaseOpcode = fileIter.next()

        if (databaseOpcode == REDIS_RDB_OPCODE_RESIZEDB) {

            val hashTableSize = fileIter.next()
            val expireTableSize = fileIter.next()

            for (index <- 0 until hashTableSize) {

                var databaseEntry = fileIter.next()
                var expireTimeMillis: Long = 0
                var expireTimeInt: Long = 0

                if (databaseEntry == REDIS_RDB_OPCODE_EXPIRETIME_MS) {
                    expireTimeMillis = readEightByteLittleEndian(fileIter)
                    databaseEntry = fileIter.next()
                }
                else if (databaseEntry == REDIS_RDB_OPCODE_EXPIRETIME) {
                    expireTimeInt = readFourByteLittleEndian(fileIter)
                    databaseEntry = fileIter.next()
                }

                val valueTypeEncoding = databaseEntry
                val key = readString(fileIter)
                val value = valueTypeEncoding match {
                    case 0 => readString(fileIter)
                }

                tempCache += (key -> value)

                if (expireTimeMillis > 0) {
                    tempExpiryCache += (key -> expireTimeMillis)
                }
                else if (expireTimeInt > 0) {
                    tempExpiryCache += (key -> expireTimeInt * 1000)
                }
                
            }
        }

    }

    private def processChecksum(fileIter: Iterator[Int]): Unit = {
        // Will implement check later
        (1 to 9).map(_ => if (fileIter.hasNext) fileIter.next().toByte else 0.toByte).toArray
        println("Reached the end of the RDB file")
    }

    def parse(filePath: String): Unit = {
        val fileSource = new FileInputStream(filePath)
        parseFile(fileSource)
    }
    
    def parseFile(fileSource: FileInputStream): Unit = {

        val fileIter = Stream.continually(fileSource.read).takeWhile(_ != -1).iterator
        readMagicString(fileIter)
        readVersion(fileIter)

        var index = 0

        while (fileIter.hasNext) {

            val opcode = fileIter.next()
            // println(s"Found opcode $opcode at iteration $index")
            index += 1
            opcode match {
                case REDIS_RDB_OPCODE_META => readMetadata(fileIter)
                case REDIS_RDB_OPCODE_SELECTDB => processDatabase(fileIter)
                case REDIS_RDB_OPCODE_EOF => processChecksum(fileIter)
                case _ => throw new RuntimeException(s"Unknown opcode $opcode")
            }

        }

        println(s"Magic string is: $magicString")
        println(s"Version is: $redisVersion")

        fileSource.close()

    }
}

// object Main {
//   def main(args: Array[String]): Unit = {
//     val parser = new RdbParser()
//     parser.parse("dump.rdb")
//   }
// }