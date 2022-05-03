package com.navercorp


import com.navercorp.graph.GraphOps
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
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
  var partitioner: Partitioner = _

  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param
    this.NUM_PARTITIONS = param.numPartition
    this.partitioner = new HashPartitioner(this.NUM_PARTITIONS)
    this
  }

  def load(): this.type = {
    val bcEdgeCreator = context.broadcast(
      if (config.directed) GraphOps.createDirectedEdge
      else GraphOps.createUndirectedEdge
    )

    val inputTriplets: RDD[(Long, Long, Double)] =
      if (config.indexed) readIndexedGraph(config.input)
      else indexingGraph(config.input)

    indexedNodes = inputTriplets.flatMap { case (srcId, dstId, weight) =>
      bcEdgeCreator.value.apply(srcId, dstId, weight)
    }.reduceByKey(_++_).partitionBy(partitioner).cache()

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

    val randomPaths: RDD[Iterable[String]] = randomWalkPaths.map {
      case (vertexId, pathBuffer) =>
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
      this.node2id = context.textFile(config.nodePath, NUM_PARTITIONS).map {
        node2index: String =>
          val Array(strNode, index) = node2index.split("\\s")
          (strNode, index.toLong)
      }
    } catch {
      case _: Exception => logger.info("Failed to read node2index file.")
      this.node2id = null
    }
    this
  }

  def readIndexedGraph(tripletPath: String): RDD[(VertexId, VertexId, Double)] = {
    val bcWeighted: Broadcast[Boolean] = context.broadcast(config.weighted)

    val rawTriplets: RDD[String] = context.textFile(tripletPath, NUM_PARTITIONS)

    if (config.nodePath == null) {
      this.node2id = createNode2Id(rawTriplets.map { triplet: String =>
        val parts: Array[String] = triplet.split("\\s")
        (parts.head, parts(1), 1)
      })
    } else {
      loadNode2Id(config.nodePath)
    }

    rawTriplets.map { triplet: String =>
      val parts: Array[String] = triplet.split("\\s")
      val weight: Double =
        if (bcWeighted.value) Try(parts.last.toDouble).getOrElse(1.0) else 1.0

      (parts.head.toLong, parts(1).toLong, weight)
    }
  }


  def indexingGraph(rawTripletPath: String): RDD[(VertexId, VertexId, Double)] = {
    // read raw graph
    val rawEdges: RDD[(String, String, Double)] =
      context.textFile(rawTripletPath, NUM_PARTITIONS).map { triplet: String =>
        val parts: Array[String] = triplet.split("\\s")
        Try(parts.head, parts(1), Try(parts.last.toDouble).getOrElse(1.0)).getOrElse(null)
      }.filter((_: (String, String, Double))!=null).repartition(NUM_PARTITIONS)

    // get map of node2id
    val partitioner = new HashPartitioner(NUM_PARTITIONS)
    this.node2id = createNode2Id(rawEdges).partitionBy(partitioner)

    val srcRDD: RDD[(String, (String, Double))] =
      rawEdges.map { case (src, dst, weight) =>
        (src, (dst, weight))
      }.partitionBy(partitioner)

    // map src node to srcId to get the dstRDD
    val dstRDD: RDD[(String, (VertexId, Double))] =
      this.node2id.join(srcRDD, partitioner).mapPartitions{
        (_: Iterator[(String, (VertexId, (String, Double)))]).map{
          case (src, (srcId, (dst, weight))) => Try(dst, (srcId, weight)).getOrElse(null)
        }.filter((_: (String, (VertexId, Double))) != null)
      }

    // map dst node to dstId to get the result
    this.node2id.join(dstRDD, partitioner).mapPartitions{
      (_: Iterator[(String, (VertexId, (VertexId, Double)))]).map{
        case (dst, (dstId, (srcId, weight))) => Try(srcId, dstId , weight).getOrElse(null)
      }.filter((_: (VertexId, VertexId, Double)) != null)
    }
  }

  def createNode2Id[T](triplets: RDD[(String, String, T)]): RDD[(String, VertexId)] =
    triplets.flatMap { case (src, dst, _) =>
      Try(Array(src, dst)).getOrElse(Array.empty[String])
    }.distinct().zipWithIndex()

}
