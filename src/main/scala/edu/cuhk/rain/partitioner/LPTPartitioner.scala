package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

class LPTPartitioner (
                       val numPartition: Int,
                       val context: SparkContext
                     ) extends PartitionerProducer {
  super.setup(context)

  val partitions: Array[LPTPartition] = Array.tabulate(numPartition)(_ => LPTPartition())
  private var _numNodes: Int = 0


  def partition(graph: Graph): this.type = {
    val it: Iterator[(Int, Int)] =
      graph.toNeighbors.mapValues(
        _.length
      ).sortBy(
        _._2, ascending = false
      ).toLocalIterator

    it.foreach { case (u, vs) => addNode(u, vs); _numNodes += 1 }
    this
  }

  private def addNode(u: Int, num: Int): Unit =
    partitions.minBy(_.calc(num)).addNode(u, num)

  def node2id: RDD[(Int, Int)] = node2id[LPTPartition](partitions, thresholds, numPartition)

  override def numNodes: Int = _numNodes

  lazy val partitioner: Partitioner = new IndexPartitioner(thresholds)

  lazy val thresholds: Array[Int] = {
    val a = new Array[Int](numPartition)
    var cnt = 0
    for (i <- 0 until numPartition) {
      a(i) = cnt
      cnt += partitions(i).numNodes
    }
    a
  }

  override def node2partition: Map[Int, Int] = {
    partitions.flatMap{partition =>
      val id: Int = partition.id
      partition.mapNodes((_, id))
    }.toMap
  }
}
