package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import edu.cuhk.rain.util.ParamsPaser.Params
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.util.LongAccumulator

import scala.annotation.tailrec

case object PartitionerTester extends Logging{
  var context: SparkContext = _
  var config: Params = _

  def setup(context: SparkContext, param: Params): this.type = {
    this.context = context
    this.config = param
    this
  }

  def partition(graph: Graph): this.type = {
    logWarning("begin partition")
    val partitioner: LPTPartitioner =
      new LPTPartitioner(config.partitions).partition(graph)
    logWarning("end partition")

    val node2partition: Map[Int, Int] = partitioner.node2partition
    val bcMap: Broadcast[Map[Int, Int]] = context.broadcast(node2partition)
    val sum: LongAccumulator = context.longAccumulator("cut")

    graph.toEdgeTriplet.foreachPartition { it =>
      var s = 0
      it.foreach { case (u, v, _) =>
        if (bcMap.value(u) != bcMap.value(v)) s += 1
      }
      sum.add(s)
    }

    logWarning(s"#cut_edges: ${sum.value}")
    logWarning(s"cut rate: ${sum.value.toDouble / graph.toEdgeTriplet.count()}")
    this
  }

  def partition(): this.type = {
    val graph: Graph = Graph.setup(context, config).fromFile()
    partition(graph)
  }
}
