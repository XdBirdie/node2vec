package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class LDGPartitioner(
                      val numPartition: Int,
                      val context: SparkContext
                    ) extends PartitionerProducer {
  super.setup(context)

  val partitions: Array[LDGPartition] = Array.tabulate(numPartition)(_ => LDGPartition())
  private var _numNodes: Int = 0

  def partition(graph: Graph): this.type = {
    _numNodes = 0
    val capacity: Int =
      (graph.numEdges.toDouble / numPartition + 1).toInt * 2

    partitions.foreach(_.setup(capacity))
    graph.toNeighbors.toLocalIterator.foreach { case (u, vs) =>
      addNode(u, vs)
      _numNodes += 1
    }
    this
  }

  private def addNode(u: Int, vs: Array[Int]): Unit =
    partitions.maxBy(_.calc(vs)).addNode(u, vs.length)

  def node2id: RDD[(Int, Int)] = node2id[LDGPartition](partitions, thresholds, numPartition)

  def numNodes: Int = _numNodes

  lazy val partitioner = new IndexPartitioner(thresholds)

  lazy val thresholds: Array[Int] = {
    val a = new Array[Int](numPartition)
    a(0) = 0
    var i = 1
    while (i < numPartition) {
      a(i) = a(i - 1) + partitions(i - 1).numNodes
      i += 1
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
