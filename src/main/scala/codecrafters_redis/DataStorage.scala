package codecrafters_redis

trait DataStorage {
  def setValue(key: String, value: String): Unit
  def getValue(key: String): String
}

object InMemoryDataStorage extends DataStorage {
  
  val mapMut = scala.collection.mutable.Map[String, String]()

  def setValue(key: String, value: String): Unit = {
    mapMut += (key -> value)
  }

  def getValue(key: String): String = {
    mapMut(key)
  }

}