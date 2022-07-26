package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import edu.cuhk.rain.util.ParamsPaser.Params
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.util.LongAccumulator

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
    val producer: PartitionerProducer =
      new LDGPartitioner(config.partitions, context).partition(graph)
    logWarning("end partition")

    val msg: String = producer.node2id.collect().mkString(", ")
    logWarning(s"node2id: $msg")

    logWarning(s"producer.partitioner = ${producer.partitioner}")

    val node2partition: Map[Int, Int] = producer.node2partition
    logWarning(s"node2partition = $node2partition")
    
    val bcMap: Broadcast[Map[Int, Int]] = context.broadcast(node2partition)
    val sum: LongAccumulator = context.longAccumulator("cut")
    graph.toEdgeTriplet.foreachPartition { it =>
      var s = 0
      it.foreach { case (u, v, _) =>
        if (bcMap.value(u) != bcMap.value(v)) s += 1
      }
      sum.add(s)
    }
    bcMap.unpersist(false)

    logWarning(s"#cut_edges: ${sum.value}")
    logWarning(s"cut rate: ${sum.value.toDouble / graph.toEdgeTriplet.count()}")
    this
  }

  def partition(): this.type = {
    val graph: Graph = Graph.setup(context, config).fromFile()
    partition(graph)
  }
}
