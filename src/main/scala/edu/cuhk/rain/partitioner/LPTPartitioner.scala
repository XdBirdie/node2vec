package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import org.apache.spark.{Partitioner, SparkContext}

class LPTPartitioner {
  var context: SparkContext = _

  var numNodes: Int = 0

  def setup(context: SparkContext): this.type = {
    this.context = context
    this
  }

  def partition(graph: Graph): this.type = {
    val it: Iterator[(Int, Int)] =
      graph.toEdgelist.groupByKey().mapValues(
        _.toSeq.length
      ).sortBy(
        _._2, ascending = false
      ).toLocalIterator

    it.foreach { case (u, vs) => addNode(u, vs); numNodes += 1 }
    this
  }

  private def addNode(u: Int, num: Int): Unit = {
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
