package com.navercorp.N2V

import com.navercorp.Main
import com.navercorp.graph.GraphOps
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}

import scala.collection.mutable.ArrayBuffer

object N2VOne extends Node2Vec {
  var indexedNodes: RDD[(VertexId, Array[(Long, Double)])] = _
  var bcAliasTable: Broadcast[Map[VertexId, (Array[VertexId], Array[Int], Array[Double])]] = _

  var NUM_PARTITIONS = 64
  var partitioner: Partitioner = _

  def setup(context: SparkContext, param: Main.Params): this.type = {
    super.setup(context, param, getClass.getName)
    this.NUM_PARTITIONS = param.numPartitions
    this.partitioner = new HashPartitioner(this.NUM_PARTITIONS)
    this
  }

  def load(): this.type = {
    val bcEdgeCreator: Broadcast[(VertexId, VertexId, Double) => Array[(VertexId, Array[(VertexId, Double)])]] =
      context.broadcast(
        if (config.directed) GraphOps.createDirectedEdge
        else GraphOps.createUndirectedEdge
      )

    val inputTriplets: RDD[(Long, Long, Double)] =
      if (config.indexed) readIndexedGraph(config.input)
      else indexingGraph(config.input)

    indexedNodes = inputTriplets.flatMap { case (srcId, dstId, weight) =>
      bcEdgeCreator.value.apply(srcId, dstId, weight)
    }.reduceByKey(_ ++ _).partitionBy(partitioner).cache()

    indexedNodes.count()
    this
  }


  def initTransitionProb(): this.type = {
    val aliasRDD: RDD[(VertexId, (Array[VertexId], Array[Int], Array[Double]))] = {
      indexedNodes.map {
        case (vertexId: VertexId, neighbors: Array[(VertexId, Double)]) =>
          val (j, q) = GraphOps.setupAlias(neighbors)
          (vertexId, (neighbors.map((_: (VertexId, Double))._1), j, q))
      }
    }
    val aliasTable: Map[VertexId, (Array[VertexId], Array[PartitionID], Array[Double])] = aliasRDD.collect().toMap
    bcAliasTable = this.context.broadcast(aliasTable)
    this
  }

  def randomWalk(bcAliasTable: Broadcast[Map[VertexId, (Array[VertexId], Array[Int], Array[Double])]]): this.type = {
    for (iter <- 0 until config.numWalks) {
      logger.warn(s"Begin random walk: $iter")

      var prevWalk: RDD[(Long, ArrayBuffer[Long])] = null
      var randomWalk: RDD[(VertexId, ArrayBuffer[VertexId])] =
        indexedNodes.map { case (nodeId: VertexId, _) =>
          val pathBuffer = new ArrayBuffer[VertexId]()
          pathBuffer.append(nodeId)
          (nodeId, pathBuffer)
        }.cache()

      randomWalk.count()

      for (walkCount <- 0 until config.walkLength) {
        prevWalk = randomWalk

        randomWalk = randomWalk.mapPartitions(
          (_: Iterator[(VertexId, ArrayBuffer[VertexId])]).map { case (srcId, pathBuffer) =>
            val currentNodeId: VertexId = pathBuffer.last

            val (neighbours, j, q) = bcAliasTable.value(currentNodeId)
            val nextNodeIndex: Int = GraphOps.drawAlias(j, q)
            val nextNodeId: VertexId = neighbours(nextNodeIndex)
            pathBuffer.append(nextNodeId)

            (srcId, pathBuffer)
          }).cache()

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

      logger.warn(s"End random walk: $iter")
    }
    this
  }

  def randomWalk(): this.type = {
    logger.warn("N2VOne Begin random walk")
    randomWalk(this.bcAliasTable)
    logger.warn("N2VOne End random walk")
    this
  }

  def cleanup(): this.type = {
    node2id.unpersist(blocking = false)
    indexedNodes.unpersist(blocking = false)
    randomWalkPaths.unpersist(blocking = false)
    this
  }
}
