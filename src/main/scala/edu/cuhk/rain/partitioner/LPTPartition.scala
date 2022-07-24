package edu.cuhk.rain.partitioner

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class LPTPartition private (val id: Int) extends Partition {
  val nodes = new ArrayBuffer[Int]
  var _size = 0

  def calc(n: Int): Int = n + _size

  def addNode(node: Int, cnt: Int): this.type = {
    nodes.append(node)
    _size += cnt
    this
  }

  def size: Int = _size

  def numNodes: Int = nodes.size

  override def mapNodes[T: ClassTag](f: Int => T): Array[T] = nodes.toArray.map(f)

  override def toString: String = s"LPTPartition: [${nodes.mkString(", ")}]"
}

object LPTPartition {
  var cnt = 0

  def apply(): LPTPartition = {
    val res = new LPTPartition(cnt)
    cnt += 1
    res
  }

  def create: LPTPartition = {
    val res = new LPTPartition(cnt)
    cnt += 1
    res
  }
}