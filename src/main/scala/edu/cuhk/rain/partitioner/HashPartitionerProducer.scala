package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.immutable

class HashPartitionerProducer(
                      val numPartition: Int,
                      val context: SparkContext
                    ) extends PartitionerProducer {
  super.setup(context)

  val partitions: Array[LDGPartition] = Array.tabulate(numPartition)(_ => LDGPartition())

  private var graph: Graph = _
  private var _numNodes: Int = 0
  private var n2p: RDD[(Int, Int)] = _

  def partition(graph: Graph): this.type = {
    this.graph = graph
    _numNodes = graph.nodelist.count().toInt

    val bcP: Broadcast[HashPartitioner] = context.broadcast(partitioner)
    n2p = graph.toNeighbors.keys.mapPartitions(
      _.map{x => (x, bcP.value.getPartition(x))}
    ).cache()
    n2p.count()
    bcP.unpersist(false)
    this
  }

  def node2id: RDD[(Int, Int)] = {
    val p2n: RDD[(Int, Array[Int])] = n2p.mapPartitions(
      _.map(_.swap)
    ).groupByKey().mapValues(_.toArray).cache()
    val tmp: Map[Int, Int] = p2n.mapValues(_.length).collect().toMap

    var sum: Int = 0
    val p2c: Map[Int, Int] = Array.tabulate(numPartition) { i =>
      val res: (Int, Int) = (i, sum); sum += tmp(i); res
    }.toMap

    println(s"p2c.toMap = ${p2c}")
    val bcP2C: Broadcast[Map[Int, Int]] = context.broadcast(p2c)
    val t: RDD[(Int, Int)] = p2n.mapPartitions(_.flatMap { case (pid, nodes) =>
      var i: Int = bcP2C.value(pid)
      nodes.map { node =>
        val res: (Int, Int) = (node, i)
        i += 1
        res
      }
    }).cache()
    t.count()
    p2n.unpersist(false)
    bcP2C.unpersist(false)
    t
  }

  def numNodes: Int = _numNodes

  lazy val partitioner = new HashPartitioner(numPartition)

  override def node2partition: Map[Int, Int] =
    this.n2p.collect().toMap

}
