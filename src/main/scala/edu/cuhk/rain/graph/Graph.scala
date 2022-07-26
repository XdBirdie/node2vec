package edu.cuhk.rain.graph

import edu.cuhk.rain.distributed.DistributedSparseMatrix
import edu.cuhk.rain.util.ParamsPaser.Params
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.util.Try

class Graph private(
                     val directed: Boolean,
                     val numPartitions: Int
                   ) {

  lazy val numEdges: Long =
    if (this.weighted) toEdgeTriplet.count()
    else toEdgelist.count()

  lazy val nodelist: RDD[Int] = {
    toEdgelist.flatMap{case (u, v) => Array(u, v)}.distinct(numPartitions)
  }

  lazy val toAdj: RDD[(Int, Array[(Int, Double)])] = {
    val bcEdgeCreator: Broadcast[(Int, Int, Double) => Array[(Int, Array[(Int, Double)])]] =
      context.broadcast(
        if (directed) createDirectedEdge
        else createUndirectedEdge
      )
    val res: RDD[(Int, Array[(Int, Double)])] =
      toEdgeTriplet.mapPartitions {
        _.flatMap {
          case (u, v, w) => bcEdgeCreator.value(u, v, w)
        }
      }.reduceByKey(_ ++ _).cache()
    bcEdgeCreator.unpersist(false)
    res
  }

  lazy val toEdgelist: RDD[(Int, Int)] = {
    (if (weighted)
      edgeTriplet.mapPartitions(it => it.map(e => (e._1, e._2)))
    else edgelist)
  }.cache()

  lazy val toEdgeTriplet: RDD[(Int, Int, Double)] = {
    (if (weighted) edgeTriplet
    else edgelist.mapPartitions(it => it.map(e => (e._1, e._2, 1.0))))
  }.cache()

  lazy val toNeighbors: RDD[(Int, Array[Int])] = {
    (if (directed) toEdgelist else toEdgelist.mapPartitions {
      _.flatMap { case (u, v) => Array((u, v), (v, u)) }
    }).groupByKey().mapValues(_.toArray)
  }.cache()

  private val context: SparkContext = Graph.context

  private val createUndirectedEdge: (Int, Int, Double) => Array[(Int, Array[(Int, Double)])]
  = (srcId: Int, dstId: Int, weight: Double) => {
    Array(
      (srcId, Array((dstId, weight))),
      (dstId, Array((srcId, weight)))
    )
  }

  private val createDirectedEdge: (Int, Int, Double) => Array[(Int, Array[(Int, Double)])]
  = (srcId: Int, dstId: Int, weight: Double) => {
    Array(
      (srcId, Array((dstId, weight)))
    )
  }
  private var weighted: Boolean = _
  private var edgeTriplet: RDD[(Int, Int, Double)] = _
  private var edgelist: RDD[(Int, Int)] = _

  def unpersist(blocking: Boolean): this.type = {
    toAdj.unpersist(blocking)
    toEdgeTriplet.unpersist(blocking)
    toEdgelist.unpersist(blocking)
    this
  }

  def toMatrix(node2id: RDD[(Int, Int)], numNodes: Int): DistributedSparseMatrix = {
    val neighbors: RDD[(Int, Int)] = toEdgelist.join(node2id).map{
      case (u, (v, uid)) => (v, uid)
    }.join(node2id).map {
      case (v, (uid, vid)) => (uid, vid)
    }
    DistributedSparseMatrix.fromEdgeList(neighbors, numNodes, directed, ids = true)
  }

  def toMatrix(node2id: Array[(Int, Int)], numNodes: Int): DistributedSparseMatrix = {
    val bcNode2id: Broadcast[Map[Int, Int]] = context.broadcast(node2id.toMap)
    val neighbors: RDD[(Int, Int)] = toEdgelist.map {
      case (u, v) => (bcNode2id.value(u), bcNode2id.value(v))
    }
    bcNode2id.unpersist(false)
    DistributedSparseMatrix.fromEdgeList(neighbors, numNodes, directed, ids = true)
  }

  private def setEdgelist(edges: RDD[(Int, Int)]): this.type = {
    this.edgelist = edges
    this.weighted = false
    this
  }

  private def setEdgeTriplet(edges: RDD[(Int, Int, Double)]): this.type = {
    this.edgeTriplet = edges
    this.weighted = true
    this
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

  def active: Graph = _active

  def fromFile(rawTripletPath: String = config.input): Graph = {
    _active = if (config.weighted) fromWeighted(rawTripletPath)
    else fromUnweighted(rawTripletPath)
    _active
  }

  private def fromUnweighted(path: String): Graph = {
    val edgelist: RDD[(Int, Int)] = context.textFile(path, config.partitions).mapPartitions {
      _.map {
        triplet: String =>
          val parts: Array[String] = triplet.split("\\s")
          (parts.head.toInt, parts(1).toInt)
      }
    }
    new Graph(config.directed, config.partitions).setEdgelist(edgelist)
  }

  private def fromWeighted(path: String): Graph = {
    val edgeTriplet: RDD[(Int, Int, Double)] = context.textFile(path, config.partitions).mapPartitions {
      _.map {
        triplet: String =>
          val parts: Array[String] = triplet.split("\\s")
          val weight: Double = Try(parts.last.toDouble).getOrElse(1.0)
          (parts.head.toInt, parts(1).toInt, weight)
      }
    }
    new Graph(config.directed, config.partitions).setEdgeTriplet(edgeTriplet)
  }
}