package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import org.apache.spark.{Partitioner, SparkContext}

import scala.collection.mutable.ArrayBuffer

class LPTPartitioner (val numPartition: Int) {

  class Partition {
    val nodes = new ArrayBuffer[Int]
    var size = 0

    def calc(n: Int): Int = n + size

    def addNode(node: Int, cnt: Int): Unit = {
      nodes.append(node)
      size += cnt
    }
  }

  val partitions: Array[Partition] = new Array[Partition](numPartition)
  for (i <- 0 until numPartition) {
    partitions(i) = new Partition
  }

  var context: SparkContext = _
  var numNodes: Int = 0

  def setup(context: SparkContext): this.type = {
    this.context = context
    this
  }

  def partition(graph: Graph): this.type = {
    val it: Iterator[(Int, Int)] =
      graph.toNeighbors.mapValues(
        _.length
      ).sortBy(
        _._2, ascending = false
      ).toLocalIterator

    it.foreach { case (u, vs) => addNode(u, vs); numNodes += 1 }
    this
  }

  private def addNode(u: Int, num: Int): Unit = {
    partitions.minBy(_.calc(num)).addNode(u, num)
  }

  def node2partition: Map[Int, Int] = {
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
    })
  }

  def partitioner: Partitioner = new IndexPartitioner(thresholds)

  private def thresholds: Array[Int] = {
    val a = new Array[Int](numPartition)
    var cnt = 0
    for (i <- 0 until numPartition) {
      a(i) = cnt
      cnt += partitions(i).size
    }
    a
  }
}
