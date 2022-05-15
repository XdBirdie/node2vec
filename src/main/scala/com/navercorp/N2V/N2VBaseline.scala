package com.navercorp.N2V

import com.navercorp.Main
import com.navercorp.graph.{EdgeAttr, GraphOps, NodeAttr}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object N2VBaseline extends Node2Vec {
  var indexedEdges: RDD[Edge[EdgeAttr]] = _
  var indexedNodes: RDD[(VertexId, NodeAttr)] = _
  var graph: Graph[NodeAttr, EdgeAttr] = _

  var NUM_PARTITIONS = 64

  def setup(context: SparkContext, param: Main.Params): this.type = {
    super.setup(context, param, getClass.getName)
    this.NUM_PARTITIONS = param.numPartitions
    this
  }

  def load(): this.type = {
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
    }.repartition(NUM_PARTITIONS).cache

    indexedEdges = indexedNodes.flatMap { case (srcId, clickNode) =>
      clickNode.neighbors.map { case (dstId, weight) =>
        Edge(srcId, dstId, EdgeAttr())
      }
    }.repartition(NUM_PARTITIONS).cache

    this
  }

  def initTransitionProb(): this.type = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)

    graph = Graph(indexedNodes, indexedEdges)
      .mapVertices[NodeAttr] { case (vertexId, clickNode) =>
        val (j, q) = GraphOps.setupAlias(clickNode.neighbors)
        val nextNodeIndex = GraphOps.drawAlias(j, q)
        clickNode.path = Array(vertexId, clickNode.neighbors(nextNodeIndex)._1)

        clickNode
      }
      .mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
        val (j, q) = GraphOps.setupEdgeAlias(bcP.value, bcQ.value)(edgeTriplet.srcId, edgeTriplet.srcAttr.neighbors, edgeTriplet.dstAttr.neighbors)
        edgeTriplet.attr.J = j
        edgeTriplet.attr.q = q
        edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1)

        edgeTriplet.attr
      }.cache

    this
  }

  def randomWalk(): this.type = {
    logger.warn("Begin random walk")
    val edge2attr = graph.triplets.map { edgeTriplet =>
      (s"${edgeTriplet.srcId}${edgeTriplet.dstId}", edgeTriplet.attr)
    }.repartition(NUM_PARTITIONS).cache
    edge2attr.count()

    for (iter <- 0 until config.numWalks) {
      logger.warn(s"Begin random walk $iter")
      var prevWalk: RDD[(Long, ArrayBuffer[Long])] = null
      var randomWalk = graph.vertices.map { case (nodeId, clickNode) =>
        val pathBuffer = new ArrayBuffer[Long]()
        pathBuffer.append(clickNode.path: _*)
        (nodeId, pathBuffer)
      }.cache
      randomWalk.count()
      graph.unpersist(blocking = false)
      graph.edges.unpersist(blocking = false)
      for (walkCount <- 0 until config.walkLength) {
        prevWalk = randomWalk
        randomWalk = randomWalk.map { case (srcNodeId, pathBuffer) =>
          val prevNodeId: VertexId = pathBuffer(pathBuffer.length - 2)
          val currentNodeId: VertexId = pathBuffer.last

          (s"$prevNodeId$currentNodeId", (srcNodeId, pathBuffer))
        }.join(edge2attr).map { case (edge, ((srcNodeId, pathBuffer), attr)) =>
          try {
            val nextNodeIndex: Int = GraphOps.drawAlias(attr.J, attr.q)
            val nextNodeId: VertexId = attr.dstNeighbors(nextNodeIndex)
            pathBuffer.append(nextNodeId)

            (srcNodeId, pathBuffer)
          } catch {
            case e: Exception => throw new RuntimeException(e.getMessage)
          }
        }.cache

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
