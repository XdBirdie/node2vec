package edu.cuhk.rain.graph

import edu.cuhk.rain.distributed.DistributedSparseMatrix
import edu.cuhk.rain.util.ParamsPaser.Params
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

class Graph private(
                   private val directed: Boolean
                   ) {

  private def setEdgelist(edges: RDD[(Long, Long)]): this.type = {
    this.edgelist = edges
    this.weighted = false
    this
  }

  private def setEdgeTriplet(edges: RDD[(Long, Long, Double)]): this.type = {
    this.edgeTriplet = edges
    this.weighted = true
    this
  }

  private val context: SparkContext = Graph.context

  private var weighted: Boolean = _
  private var edgeTriplet: RDD[(Long, Long, Double)] = _
  private var edgelist: RDD[(Long, Long)] = _

  lazy val numEdges: Long =
    if (this.weighted) toEdgeTriplet.count()
    else toEdgelist.count()

  lazy val toAdj: RDD[(Long, Array[(Long, Double)])] = {
    val edgeCreator: (Long, Long, Double) => Array[(Long, Array[(Long, Double)])] =
      if (directed) createDirectedEdge else createUndirectedEdge
    val bcEdgeCreator: Broadcast[(Long, Long, Double) => Array[(Long, Array[(Long, Double)])]] =
      context.broadcast(edgeCreator)

    toEdgeTriplet.mapPartitions {
      _.flatMap { case (u, v, w) => bcEdgeCreator.value(u, v, w)}
    }.reduceByKey(_ ++ _).cache()
  }

  lazy val toEdgelist: RDD[(Long, Long)] = {
    (if (weighted)
      edgeTriplet.mapPartitions(it => it.map(e => (e._1, e._2)))
    else edgelist).cache()
  }

  lazy val toEdgeTriplet: RDD[(Long, Long, Double)] = {
    (if (weighted) edgeTriplet
    else edgelist.mapPartitions(it => it.map(e => (e._1, e._2, 1.0)))).cache()
  }

  lazy val toNeighbors: RDD[(Long, Array[Long])] = {
    (if (directed) toEdgelist else toEdgelist.mapPartitions{
      _.flatMap{case (u, v) => Array((u, v), (v, u))}
    }).groupByKey().mapValues(_.toArray).cache()
  }

  def unpersist(blocking: Boolean): this.type = {
    toAdj.unpersist(blocking)
    toEdgeTriplet.unpersist(blocking)
    toEdgelist.unpersist(blocking)
    this
  }

  private val createUndirectedEdge: (Long, Long, Double) => Array[(Long, Array[(Long, Double)])]
    = (srcId: Long, dstId: Long, weight: Double) => {
      Array(
        (srcId, Array((dstId, weight))),
        (dstId, Array((srcId, weight)))
      )
    }

  private val createDirectedEdge: (Long, Long, Double) => Array[(Long, Array[(Long, Double)])]
    = (srcId: Long, dstId: Long, weight: Double) => {
      Array(
        (srcId, Array((dstId, weight)))
      )
    }
}

object Graph {
  private var context: SparkContext = _
  private var config: Params = _
  private var _active: Graph = _

  def setup(context: SparkContext, param: Params): this.type = {
    this.context = context
    this.config = param
    this
  }

  def mapNode2Id(graph: Graph, node2id: RDD[(Long, Int)]) = {

  }

  def active: Graph = _active

  def fromFile(rawTripletPath: String=config.input): Graph = {
    _active = if (config.weighted) fromWeighted(rawTripletPath)
    else fromUnweighted(rawTripletPath)
    _active
  }

  private def fromUnweighted(path: String): Graph = {
    val edgelist: RDD[(Long, Long)] = context.textFile(path, config.partitions).mapPartitions {
      _.map {
        triplet: String =>
          val parts: Array[String] = triplet.split("\\s")
          (parts.head.toLong, parts(1).toLong)
      }
    }
    new Graph(config.directed).setEdgelist(edgelist)
  }

  private def fromWeighted(path: String): Graph = {
    val edgeTriplet: RDD[(Long, Long, Double)] = context.textFile(path, config.partitions).mapPartitions{
      _.map {
        triplet: String =>
          val parts: Array[String] = triplet.split("\\s")
          val weight: Double = Try(parts.last.toDouble).getOrElse(1.0)
          (parts.head.toLong, parts(1).toLong, weight)
      }
    }
    new Graph(config.directed).setEdgeTriplet(edgeTriplet)
  }
}