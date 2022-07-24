package edu.cuhk.rain.partitioner

import scala.collection.mutable
import scala.reflect.ClassTag

class LDGPartition private(val id: Int) extends Partition {
  val nodes = new mutable.TreeSet[Int]()
  var capacity: Int = 0

  private var _size: Int = 0

  def setup(capacity: Int): this.type = {
    nodes.clear()
    this.capacity = capacity
    this
  }

  def addNode(node: Int, cnt: Int): this.type = {
    nodes.add(node); _size += cnt; this
  }

  def calc(neighbours: Array[Int]): Double = {
    var cnt = 1
    neighbours.foreach(u => if (nodes.contains(u)) cnt += 1)
    val w: Double = 1 - (size + neighbours.length).toDouble / capacity
    (if (w > 0) cnt else 1 / cnt) * w
  }

  def size: Int = _size

  def numNodes: Int = nodes.size

  override def mapNodes[T: ClassTag](f: Int => T): Array[T] = nodes.toArray.map(f)

  override def toString: String = s"LDGPartition: [${nodes.mkString(", ")}]"
}

object LDGPartition {
  var cnt = 0

  def apply(): LDGPartition = {
    val res = new LDGPartition(cnt)
    cnt += 1
    res
  }
}
