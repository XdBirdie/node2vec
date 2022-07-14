package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import edu.cuhk.rain.util.ParamsPaser.Params
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator

case object Partitioner {
  var context: SparkContext = _
  var config: Params = _

  def setup(context: SparkContext, param: Params): this.type = {
    this.context = context
    this.config = param
    this
  }

  def partition(graph: Graph): this.type = {
    this
  }

  def partition(): Unit = {
    val graph: Graph = Graph.setup(context, config).fromFile()

    //    val adj: RDD[(Long, Array[(Long, Double)])] = graph.toAdj
    //    val tuples: Array[(Long, Array[Long])] = adj.mapValues(_.map(_._1)).collect()
    val partitioner = new LDGPartitioner(config.partitions)
    partitioner.partition(graph)
    //    tuples.foreach{case (u, vs) => partitioner.addNode(u, vs)}

    val node2partition: Map[Int, Int] = partitioner.node2partition
    println(node2partition.size)
    //    node2partition.foreach(println)

    //    var sum = 0
    //    tuples.foreach{case (u, vs) => vs.foreach{v =>
    //      if (node2partition(u) != node2partition(v)) sum += 1
    //    }}

    val bcMap: Broadcast[Map[Int, Int]] = context.broadcast(node2partition)
    val sum: LongAccumulator = context.longAccumulator("cut")
    //    graph.toEdgelist.collect().foreach(println)
    graph.toEdgeTriplet.foreachPartition { it =>
      var s = 0
      it.foreach { case (u, v, _) =>
        if (bcMap.value(u) != bcMap.value(v)) s += 1
      }
      sum.add(s)
    }
    println(sum.value)
    println(sum.value.toDouble / graph.toEdgeTriplet.count())
  }
}
