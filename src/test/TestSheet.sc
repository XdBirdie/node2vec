import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer

case object TimeRecorder {
  case class Record(time: Long, msg: String)

  val startTime: Long = System.currentTimeMillis()
  val timeRecords = new ArrayBuffer[Record]

  def init(): Long = startTime

  def apply(msg: String): Record = {
    val currentTime: Long = System.currentTimeMillis()
    val record: Record = Record(currentTime - startTime, msg)
    val t = Thread.currentThread.getStackTrace()(2)
    println(s"${t.getMethodName} - ${t.getLineNumber}")
    timeRecords += record
    record
  }

  def show(): Unit = {
    timeRecords.foreach {record: Record =>
      println(s"${record.time} ${record.msg}")
    }

  }

  def save(filePath: String): Unit = {
    val writer = new PrintWriter(filePath)
    timeRecords.foreach {record: Record =>
      writer.println(s"${record.time} ${record.msg}")
    }
    writer.close()
  }
}

TimeRecorder.init()

def test():Unit = {
  println(TimeRecorder.apply("test1"))
  println(TimeRecorder.apply("test2"))
}

test()

TimeRecorder.show()

TimeRecorder.save("test.txt")

//TimeRecorder.startTime = 10