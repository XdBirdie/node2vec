package com.navercorp.util

import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer

case object TimeRecorder {
  case class Record(time: Long, msg: String, caller: String) {
    override def toString: String = s"${time}\t ${caller}\t - ${msg}"

  }

  val startTime: Long = System.currentTimeMillis()
  val timeRecords = new ArrayBuffer[Record]

  def init(): Long = startTime

  def apply(msg: String): Record = {
    val delta: Long = System.currentTimeMillis() - startTime
    val caller: String = getTrace(3)

    val record: Record = Record(delta, msg, caller)
    timeRecords += record
    record
  }

  def show(): Unit = {
    timeRecords foreach println
  }

  def save(filePath: String): Unit = {
    val writer = new PrintWriter(filePath)
    timeRecords.foreach (writer.println)
    writer.close()
  }

  private def getTrace(level: Int):String = {
    val t: StackTraceElement = Thread.currentThread.getStackTrace()(level)
    s"${t.getFileName}:${t.getLineNumber}"
  }
}
