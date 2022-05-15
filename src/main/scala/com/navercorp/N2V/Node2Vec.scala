package com.navercorp.N2V

import com.navercorp.{Main, word2vec}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

trait Node2Vec extends Serializable {
  var logger: Logger = _
  var context: SparkContext = _
  var config: Main.Params = _

  var randomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = _

  var node2id: RDD[(String, Long)] = _

  protected def setup(context: SparkContext, param: Main.Params, className: String): this.type = {
    this.context = context
    this.config = param
    this.logger = LoggerFactory.getLogger(className)
    this
  }

  def setup(context: SparkContext, param: Main.Params): this.type

  def load(): this.type

  def initTransitionProb(): this.type

  def randomWalk(): this.type

  def embedding(): this.type = {
    val randomPaths: RDD[Iterable[String]] = randomWalkPaths.map {
      case (_, pathBuffer) =>
        Try(pathBuffer.map((_: VertexId).toString).toIterable).getOrElse(null)
    }.filter((_: Iterable[String]) != null)
    word2vec.setup(context, config).fit(randomPaths)
    this
  }

  def save(): this.type = {
    this.saveRandomPath()
      .saveModel()
      .saveVectors()
  }

  def saveRandomPath(): this.type = {
    randomWalkPaths
      .map{case (_, pathBuffer) => Try(pathBuffer.mkString("\t")).getOrElse(null)}
      .filter((x: String) => x != null && x.replaceAll("\\s", "").nonEmpty)
      .repartition(config.numPartitions)
      .saveAsTextFile(config.output)
    this
  }

  def saveModel(): this.type = {
    word2vec.save(config.output)
    this
  }

  def saveVectors(): this.type = {
    val partitioner = new HashPartitioner(config.numPartitions)

    val node2vector: RDD[(VertexId, String)] =
      context.makeRDD(word2vec.getVectors.toList, config.numPartitions).map{
        case (nodeId, vector) => (nodeId.toLong, vector.mkString(", "))
      }.partitionBy(partitioner)

    if (this.node2id != null) {
      val id2Node: RDD[(VertexId, String)] = this.node2id.map { case (strNode, index) =>
        (index, strNode)
      }.partitionBy(partitioner)

      node2vector.join(id2Node, partitioner)
        .map { case (_, (vector, name)) => s"$name\t$vector" }
        .repartition(config.numPartitions)
        .saveAsTextFile(s"${config.output}.emb")
    } else {
      node2vector.map { case (nodeId, vector) => s"$nodeId\t$vector" }
        .repartition(config.numPartitions)
        .saveAsTextFile(s"${config.output}.emb")
    }

    this
  }

  def loadNode2Id(node2idPath: String): this.type = {
    try {
      this.node2id = context.textFile(config.nodePath, config.numPartitions).map {
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

    val rawTriplets: RDD[String] = context.textFile(tripletPath, config.numPartitions)

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
      context.textFile(rawTripletPath, config.numPartitions).map { triplet: String =>
        val parts: Array[String] = triplet.split("\\s")
        Try(parts.head, parts(1), Try(parts.last.toDouble).getOrElse(1.0)).getOrElse(null)
      }.filter((_: (String, String, Double)) != null).repartition(config.numPartitions)

    // get map of node2id
    val partitioner = new HashPartitioner(config.numPartitions)
    this.node2id = createNode2Id(rawEdges).partitionBy(partitioner)

    val srcRDD: RDD[(String, (String, Double))] =
      rawEdges.map { case (src, dst, weight) =>
        (src, (dst, weight))
      }.partitionBy(partitioner)

    // map src node to srcId to get the dstRDD
    val dstRDD: RDD[(String, (VertexId, Double))] =
      this.node2id.join(srcRDD, partitioner).mapPartitions {
        (_: Iterator[(String, (VertexId, (String, Double)))]).map {
          case (src, (srcId, (dst, weight))) => Try(dst, (srcId, weight)).getOrElse(null)
        }.filter((_: (String, (VertexId, Double))) != null)
      }

    // map dst node to dstId to get the result
    this.node2id.join(dstRDD, partitioner).mapPartitions {
      (_: Iterator[(String, (VertexId, (VertexId, Double)))]).map {
        case (dst, (dstId, (srcId, weight))) => Try(srcId, dstId, weight).getOrElse(null)
      }.filter((_: (VertexId, VertexId, Double)) != null)
    }
  }

  def createNode2Id[T](triplets: RDD[(String, String, T)]): RDD[(String, VertexId)] =
    triplets.flatMap { case (src, dst, _) =>
      Try(Array(src, dst)).getOrElse(Array.empty[String])
    }.distinct().zipWithIndex()

}
