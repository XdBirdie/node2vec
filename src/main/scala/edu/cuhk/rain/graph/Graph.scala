package edu.cuhk.rain.graph

import edu.cuhk.rain.util.ParamsPaser.Params
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

case object Graph {
  private var context: SparkContext = _
  private var config: Params = _
  private var edgeTriplet: RDD[(Long, Long, Double)] = _

  def setup(context: SparkContext, param: Params): this.type = {
    this.context = context
    this.config = param
    this
  }

  def loadTriplet(rawTripletPath: String=config.input): this.type = {
    val bcWeighted: Broadcast[Boolean] = context.broadcast(config.weighted)
    edgeTriplet = context.textFile(rawTripletPath, config.partitions).mapPartitions {_.map {
      triplet: String =>
        val parts: Array[String] = triplet.split("\\s")
        val weight: Double = if (bcWeighted.value) Try(parts.last.toDouble).getOrElse(1.0) else 1.0
        (parts.head.toLong, parts(1).toLong, weight)
    }}.cache()
    edgeTriplet.unpersist()
    this
  }

  def toAdj: RDD[(Long, Array[(Long, Double)])] = {
    val edgeCreator: (Long, Long, Double) => Array[(Long, Array[(Long, Double)])] =
      if (config.directed) createDirectedEdge else createUndirectedEdge
    val bcEdgeCreator: Broadcast[(Long, Long, Double) => Array[(Long, Array[(Long, Double)])]] =
      context.broadcast(edgeCreator)

    edgeTriplet.mapPartitions {
      _.flatMap { case (u, v, w) => bcEdgeCreator.value(u, v, w)}
    }.reduceByKey(_ ++ _)
  }

  def unpersist(blocking: Boolean=true): Unit = edgeTriplet.unpersist(blocking)

  def edgeList: RDD[(Long, Long, Double)] = edgeTriplet

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
