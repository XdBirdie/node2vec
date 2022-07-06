package edu.cuhk.rain.partitioner

import scala.collection.mutable

class Partition (val id: Int, val capacity: Int){
  val nodes = new mutable.TreeSet[Long]()
  def size: Int = nodes.size

  def addNode(node: Long):Unit = {
    nodes.add(node)
  }

  def calc(neighbours: Array[Long]): Double = {
    var cnt = 1
    neighbours.foreach(u => if (nodes.contains(u)) cnt += 1)
    val w: Double =  1 - nodes.size.toDouble / capacity
    w * cnt
  }
}
