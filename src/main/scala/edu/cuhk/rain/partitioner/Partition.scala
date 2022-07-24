package edu.cuhk.rain.partitioner

import scala.reflect.ClassTag

trait Partition  extends Serializable {
  val id: Int

  def size: Int

  def numNodes: Int

  def addNode(u: Int, w: Int): this.type

  def mapNodes[T: ClassTag](f: Int => T): Array[T]
}
