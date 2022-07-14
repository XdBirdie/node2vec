package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Partition (val id: Int){

  def this(id: Int, capacity: Int) {
    this(id)
    this.capacity = capacity
  }

  val nodes = new mutable.TreeSet[Long]()
  var capacity: Int = _

  def size: Int = nodes.size

  def clear(): this.type = {
    nodes.clear()
    this
  }

  def setCapacity(capacity: Int): this.type = {
    this.capacity = capacity
    this
  }

  def addNode(node: Long):Unit = {
    nodes.add(node)
  }

  def calc(neighbours: Array[Long]): Double = {
    var cnt = 1
    neighbours.foreach(u => if (nodes.contains(u)) cnt += 1)
    val w: Double = 1 - (nodes.size + neighbours.length).toDouble / capacity
    (if (w > 0) cnt else 1 / cnt) * w
  }
}

class LDGPartitioner (val numPartition: Int) {

  val partitions: Array[Partition] = new Array[Partition](numPartition)

  for (i <- 0 until numPartition) {
    partitions(i) = new Partition(i)
  }

  var context: SparkContext = _

  def setup(context: SparkContext): this.type = {
    this.context = context
    this
  }

  def partition(graph: Graph): this.type = {
    val capacity: Int = math.ceil(graph.numEdges.toDouble / numPartition).toInt
    partitions.foreach(_.setCapacity(capacity))
    val it: Iterator[(Long, Array[Long])] = graph.toNeighbors.toLocalIterator
    it.foreach{case (u, vs) => addNode(u, vs)}
    this
  }

  def addNode(u: Long, vs: Array[Long]): Unit = {
    partitions.maxBy(_.calc(vs)).addNode(u)

//    var maxw: Double = Double.MinValue
//    var ind = 0
//    for (i <- 0 until numPartition) {
//      val w: Double = partitions(i).calc(vs)
//      if (w > maxw) {
//        maxw = w
//        ind = i
//      }
//    }
//    partitions(ind).addNode(u)
  }

  def node2partition: Map[Long, Int] = {
    partitions.foreach(x => println(x.size))
    partitions.zipWithIndex.flatMap{case (partition, id) =>
      partition.nodes.map((_, id))
    }.toMap
  }

  def node2id: Array[(Long, Long)] = {
    var id = 0
    partitions.flatMap(_.nodes.map{node =>
        val res: (Long, Long) = (node, id)
        id += 1
        res
      }
    )
  }

  def thresholds: Array[Int] = {
    val a = new Array[Int](numPartition)
    var cnt = 0
    for (i <- 0 until numPartition) {
      a(i) = cnt
      cnt += partitions(i).size
    }
    a
  }

  def partitioner: Partitioner = new IndexPartitioner(thresholds)


}
