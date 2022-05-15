package com.navercorp.N2V

import com.navercorp.Main
import com.navercorp.graph.{EdgeAttr, GraphOps, NodeAttr}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import scala.collection.mutable.ArrayBuffer

object N2VPartition extends Node2Vec {
  var indexedEdges: RDD[Edge[EdgeAttr]] = _
  var indexedNodes: RDD[(VertexId, NodeAttr)] = _
  var graph: Graph[NodeAttr, EdgeAttr] = _

  def setup(context: SparkContext, param: Main.Params): this.type = {
    super.setup(context, param, getClass.getName)
    this
  }

  def load(): this.type = {
    logger.warn("N2VPartition load")

    val bcMaxDegree = context.broadcast(config.degree)
    val bcEdgeCreator = config.directed match {
      case true => context.broadcast(GraphOps.createDirectedEdge)
      case false => context.broadcast(GraphOps.createUndirectedEdge)
    }

    val inputTriplets: RDD[(Long, Long, Double)] = config.indexed match {
      case true => readIndexedGraph(config.input)
      case false => indexingGraph(config.input)
    }

    indexedNodes = inputTriplets.flatMap { case (srcId, dstId, weight) =>
      bcEdgeCreator.value.apply(srcId, dstId, weight)
    }.reduceByKey(_ ++ _).map { case (nodeId, neighbors: Array[(VertexId, Double)]) =>
      var neighbors_ = neighbors
      if (neighbors_.length > bcMaxDegree.value) {
        neighbors_ = neighbors.sortWith { case (left, right) => left._2 > right._2 }.slice(0, bcMaxDegree.value)
      }

      (nodeId, NodeAttr(neighbors = neighbors_.distinct))
    }.repartition(config.numPartitions).cache

    indexedEdges = indexedNodes.flatMap { case (srcId, clickNode) =>
      clickNode.neighbors.map { case (dstId, weight) =>
        Edge(srcId, dstId, EdgeAttr())
      }
    }.repartition(config.numPartitions).cache

    this
  }


  def initTransitionProb(): this.type = {
    logger.warn("N2VPartition init transition prob")

    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)

    graph = Graph(indexedNodes, indexedEdges)
      .mapVertices[NodeAttr] { case (vertexId, clickNode) =>
        val (j, q) = GraphOps.setupAlias(clickNode.neighbors)
        val nextNodeIndex: Int = GraphOps.drawAlias(j, q)
        clickNode.path = Array(vertexId, clickNode.neighbors(nextNodeIndex)._1)

        clickNode
      }.cache()
      .mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
        val (j, q) = GraphOps.setupEdgeAlias(bcP.value, bcQ.value)(edgeTriplet.srcId, edgeTriplet.srcAttr.neighbors, edgeTriplet.dstAttr.neighbors)
        edgeTriplet.attr.J = j
        edgeTriplet.attr.q = q
        edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1)

        edgeTriplet.attr
      }.cache()
    this
  }

  def randomWalk(): this.type = {
    logger.warn("N2VPartition random walk")

    val partitioner = new HashPartitioner(config.numPartitions)

    val edge2attr: RDD[((VertexId, VertexId), EdgeAttr)] = graph.triplets.map {
      edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
        ((edgeTriplet.srcId, edgeTriplet.dstId), edgeTriplet.attr)
    }.partitionBy(partitioner).cache()
    edge2attr.count()
    logger.warn("N2VPartition edge2attr set up")

    for (iter <- 0 until config.numWalks) {
      logger.warn(s"N2VPartition begin random walk ${iter}")

      var prevWalk: RDD[(Long, ArrayBuffer[Long])] = null
      var randomWalk: RDD[(VertexId, ArrayBuffer[VertexId])] = graph.vertices.map { case (nodeId, clickNode) =>
        val pathBuffer = new ArrayBuffer[Long]()
        pathBuffer.append(clickNode.path: _*)
        (nodeId, pathBuffer)
      }.cache
      randomWalk.count()

      for (walkCount <- 0 until config.walkLength) {
        prevWalk = randomWalk
        val tempWalk = randomWalk.map { case (srcNodeId, pathBuffer) =>
          val prevNodeId: VertexId = pathBuffer(pathBuffer.length - 2)
          val currentNodeId: VertexId = pathBuffer.last

          ((prevNodeId, currentNodeId), (srcNodeId, pathBuffer))
        }.partitionBy(partitioner)

        randomWalk = edge2attr.join(tempWalk, partitioner).map {
          case (edge, (attr, (srcNodeId, pathBuffer))) =>
            val nextNodeIndex: Int = GraphOps.drawAlias(attr.J, attr.q)
            val nextNodeId: VertexId = attr.dstNeighbors(nextNodeIndex)
            pathBuffer.append(nextNodeId)

            (srcNodeId, pathBuffer)
        }.cache()

        randomWalk.count()
        prevWalk.unpersist(blocking = false)
      }

      if (randomWalkPaths != null) {
        val prevRandomWalkPaths: RDD[(VertexId, ArrayBuffer[VertexId])] = randomWalkPaths
        randomWalkPaths = randomWalkPaths.union(randomWalk).cache()
        randomWalkPaths.count()
        prevRandomWalkPaths.unpersist(blocking = false)
      } else {
        randomWalkPaths = randomWalk
      }
    }
    logger.warn(s"End random walk")
    this
  }

  def cleanup(): this.type = {
    node2id.unpersist(blocking = false)
    indexedEdges.unpersist(blocking = false)
    indexedNodes.unpersist(blocking = false)
    graph.unpersist(blocking = false)
    randomWalkPaths.unpersist(blocking = false)

    this
  }
}
