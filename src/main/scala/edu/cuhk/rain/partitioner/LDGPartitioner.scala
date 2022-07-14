package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import org.apache.spark.{Partitioner, SparkContext}

import scala.collection.mutable

class Partition(val id: Int) {

  val nodes = new mutable.TreeSet[Int]()
  var capacity: Int = _
  private var _size: Int = 0

  def this(id: Int, capacity: Int) {
    this(id)
    this.capacity = capacity
  }

  def clear(): this.type = {
    nodes.clear()
    this
  }

  def setCapacity(capacity: Int): this.type = {
    this.capacity = capacity
    this
  }

  def addNode(node: Int, cnt: Int): Unit = {
    nodes.add(node)
    _size += cnt
  }

  def calc(neighbours: Array[Int]): Double = {
    var cnt = 1
    neighbours.foreach(u => if (nodes.contains(u)) cnt += 1)
    val w: Double = 1 - (size + neighbours.length).toDouble / capacity
    (if (w > 0) cnt else 1 / cnt) * w
  }

  def size: Int = _size
}

class LDGPartitioner(val numPartition: Int) {

  val partitions: Array[Partition] = new Array[Partition](numPartition)

  for (i <- 0 until numPartition) {
    partitions(i) = new Partition(i)
  }

  var context: SparkContext = _

  var numNodes: Int = 0

  def setup(context: SparkContext): this.type = {
    this.context = context
    this
  }

  def partition(graph: Graph): this.type = {
    val capacity: Int = {
      math.ceil(graph.numEdges.toDouble / numPartition).toInt * 2
    }
    println(s"graph.numEdges = ${graph.numEdges}")
    println(s"capacity = ${capacity}")

    partitions.foreach(_.setCapacity(capacity))
    val it: Iterator[(Int, Array[Int])] = graph.toNeighbors.toLocalIterator
    it.foreach { case (u, vs) => addNode(u, vs); numNodes += 1 }
    this
  }

  def addNode(u: Int, vs: Array[Int]): Unit = {
    partitions.maxBy(_.calc(vs)).addNode(u, vs.length)
  }

  def node2partition: Map[Int, Int] = {
    partitions.foreach(x => println(x.size))
    partitions.zipWithIndex.flatMap { case (partition, id) =>
      partition.nodes.map((_, id))
    }.toMap
  }

  def node2id: Array[(Int, Int)] = {
    var id = 0
    partitions.flatMap(_.nodes.map { node =>
      val res: (Int, Int) = (node, id)
      id += 1
      res
    }
    )
  }

  def partitioner: Partitioner = new IndexPartitioner(thresholds)

  def thresholds: Array[Int] = {
    val a = new Array[Int](numPartition)
    var cnt = 0
    for (i <- 0 until numPartition) {
      a(i) = cnt
      cnt += partitions(i).size
    }
    a
  }


}
