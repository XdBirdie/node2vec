package com.navercorp


import com.navercorp.graph.GraphOps
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try



object N2VOne extends Node2Vec {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName);

  var context: SparkContext = null

  var config: Main.Params = null
  var node2id: RDD[(String, Long)] = null
  var indexedNodes: RDD[(VertexId, Array[(Long, Double)])] = _

  var randomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = null

  var bcAliasTable: Broadcast[Map[VertexId, (Array[VertexId], Array[Int], Array[Double])]] = _

  var NUM_PARTITIONS = 64

  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param
    NUM_PARTITIONS = param.numPartition
    this
  }

  def load(): this.type = {
    val bcEdgeCreator: Broadcast[(VertexId, VertexId, Double) => Array[(VertexId, Array[(VertexId, Double)])]] =
      if (config.directed) { context.broadcast(GraphOps.createDirectedEdge)}
      else { context.broadcast(GraphOps.createUndirectedEdge)}

    val inputTriplets: RDD[(Long, Long, Double)] =
      if (config.indexed) { readIndexedGraph(config.input)}
      else { indexingGraph(config.input) }

    indexedNodes = inputTriplets.flatMap { case (srcId, dstId, weight) =>
      bcEdgeCreator.value.apply(srcId, dstId, weight)
    }.reduceByKey(_++_).cache()

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

    val aliasTable: Map[VertexId, (Array[VertexId], Array[Int], Array[Double])] = aliasRDD.collect().toMap
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
        }.cache

      randomWalk.count()

      for (walkCount <- 0 until config.walkLength) {
        prevWalk = randomWalk

        randomWalk = randomWalk.mapValues { pathBuffer: ArrayBuffer[VertexId] => {
            val currentNodeId: VertexId = pathBuffer.last

            val (neighbours, j, q) = bcAliasTable.value(currentNodeId)
            val nextNodeIndex: Int = GraphOps.drawAlias(j, q)
            val nextNodeId: VertexId = neighbours(nextNodeIndex)
            pathBuffer.append(nextNodeId)

            pathBuffer
          }
        }.cache()

        randomWalk.count()
        prevWalk.unpersist(blocking=false)
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

  def embedding(): this.type = {
    logger.warn("Begin embedding")

    val randomPaths = randomWalkPaths.map { case (vertexId, pathBuffer) =>
      Try(pathBuffer.map(_.toString).toIterable).getOrElse(null)
    }.filter(_!=null)

    Word2vec.setup(context, config).fit(randomPaths)

    logger.warn("End embedding")
    this
  }

  def save(): this.type = {
    this.saveRandomPath()
      .saveModel()
      .saveVectors()
  }

  def saveRandomPath(): this.type = {
    randomWalkPaths
      .map { case (vertexId, pathBuffer) =>
        Try(pathBuffer.mkString("\t")).getOrElse(null)
      }
      .filter(x => x != null && x.replaceAll("\\s", "").length > 0)
      .repartition(NUM_PARTITIONS)
      .saveAsTextFile(config.output)

    this
  }

  def saveModel(): this.type = {
    Word2vec.save(config.output)

    this
  }

  def saveVectors(): this.type = {
    val node2vector = context.parallelize(Word2vec.getVectors.toList)
      .map { case (nodeId, vector) =>
        (nodeId.toLong, vector.mkString(","))
      }

    if (this.node2id != null) {
      val id2Node = this.node2id.map{ case (strNode, index) =>
        (index, strNode)
      }

      node2vector.join(id2Node)
        .map { case (nodeId, (vector, name)) => s"$name\t$vector" }
        .repartition(NUM_PARTITIONS)
        .saveAsTextFile(s"${config.output}.emb")
    } else {
      node2vector.map { case (nodeId, vector) => s"$nodeId\t$vector" }
        .repartition(NUM_PARTITIONS)
        .saveAsTextFile(s"${config.output}.emb")
    }

    this
  }

  def cleanup(): this.type = {
    node2id.unpersist(blocking = false)
    indexedNodes.unpersist(blocking = false)
    randomWalkPaths.unpersist(blocking = false)
    this
  }

  def loadNode2Id(node2idPath: String): this.type = {
    try {
      this.node2id = context.textFile(config.nodePath).map { node2index =>
        val Array(strNode, index) = node2index.split("\\s")
        (strNode, index.toLong)
      }
    } catch {
      case e: Exception => logger.info("Failed to read node2index file.")
        this.node2id = null
    }

    this
  }

  def readIndexedGraph(tripletPath: String) = {
    val bcWeighted: Broadcast[Boolean] = context.broadcast(config.weighted)

    val rawTriplets: RDD[String] = context.textFile(tripletPath)
    if (config.nodePath == null) {
      this.node2id = createNode2Id(rawTriplets.map { triplet =>
        val parts = triplet.split("\\s")
        (parts.head, parts(1), -1)
      })
    } else {
      loadNode2Id(config.nodePath)
    }

    rawTriplets.map { triplet =>
      val parts = triplet.split("\\s")
      val weight = bcWeighted.value match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }

      (parts.head.toLong, parts(1).toLong, weight)
    }
  }


  def indexingGraph(rawTripletPath: String): RDD[(Long, Long, Double)] = {
    val rawEdges = context.textFile(rawTripletPath).map { triplet =>
      val parts = triplet.split("\\s")

      Try {
        (parts.head, parts(1), Try(parts.last.toDouble).getOrElse(1.0))
      }.getOrElse(null)
    }.filter(_!=null)

    this.node2id = createNode2Id(rawEdges)

    rawEdges.map { case (src, dst, weight) =>
      (src, (dst, weight))
    }.join(node2id).map { case (src, (edge: (String, Double), srcIndex: Long)) =>
      try {
        val (dst: String, weight: Double) = edge
        (dst, (srcIndex, weight))
      } catch {
        case e: Exception => null
      }
    }.filter(_!=null).join(node2id).map { case (dst, (edge: (Long, Double), dstIndex: Long)) =>
      try {
        val (srcIndex, weight) = edge
        (srcIndex, dstIndex, weight)
      } catch {
        case e: Exception => null
      }
    }.filter(_!=null)
  }

  def createNode2Id[T <: Any](triplets: RDD[(String, String, T)]): RDD[(String, VertexId)] = triplets.flatMap { case (src, dst, weight) =>
    Try(Array(src, dst)).getOrElse(Array.empty[String])
  }.distinct().zipWithIndex()

}
