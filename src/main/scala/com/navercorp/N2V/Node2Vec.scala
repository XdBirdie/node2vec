package com.navercorp.N2V

import com.navercorp.util.TimeRecorder
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

  def cleanup(): this.type

  def embedding(): this.type = {
    TimeRecorder(s"embedding begin")
    val randomPaths: RDD[Iterable[String]] = randomWalkPaths.map {
      case (_, pathBuffer) =>
        Try(pathBuffer.map((_: VertexId).toString).toIterable).getOrElse(null)
    }.filter((_: Iterable[String]) != null).cache()
    randomPaths.count()
    randomWalkPaths.unpersist(blocking = false)
    word2vec.setup(context, config).fit(randomPaths)
    TimeRecorder(s"embedding end")
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
        .map { case (_, (vector, name)) => s"$name\t $vector" }
        .repartition(config.numPartitions)
        .saveAsTextFile(s"${config.output}.emb")
    } else {
      node2vector.map { case (nodeId, vector) => s"$nodeId\t $vector" }
        .repartition(config.numPartitions)
        .saveAsTextFile(s"${config.output}.emb")
    }

    this
  }

  def readIndexedGraph(tripletPath: String): RDD[(VertexId, VertexId, Double)] = {
    val bcWeighted: Broadcast[Boolean] = context.broadcast(config.weighted)

    val rawTriplets: RDD[String] = context.textFile(tripletPath, config.numPartitions).cache()

    if (config.nodePath != null) loadNode2Id()
    else this.node2id = null

    rawTriplets.map { triplet: String =>
      val parts: Array[String] = triplet.split("\\s")
      val weight: Double = if (bcWeighted.value) Try(parts.last.toDouble).getOrElse(1.0) else 1.0
      (parts.head.toLong, parts(1).toLong, weight)
    }
  }

  def indexingGraph(rawTripletPath: String): RDD[(VertexId, VertexId, Double)] = {
    val bcWeighted: Broadcast[Boolean] = context.broadcast(config.weighted)

    // read raw graph
    val rawEdges: RDD[(String, String, Double)] =
      context.textFile(rawTripletPath, config.numPartitions).mapPartitions { (_: Iterator[String]).map {
        triplet: String =>
          val parts: Array[String] = triplet.split("\\s")
          val weight: Double = if (bcWeighted.value) Try(parts.last.toDouble).getOrElse(1.0) else 1.0
          (parts.head, parts(1), weight)
      }}

    // get map of node2id
    val partitioner = new HashPartitioner(config.numPartitions)
    this.node2id = createNode2Id(rawEdges).partitionBy(partitioner)

    rawEdges.map { case (src, dst, weight) =>
      (src, (dst, weight))
    }.partitionBy(partitioner).join(this.node2id, partitioner).mapPartitions {
      (_: Iterator[(String, ((String, Double), VertexId))]).map {
        case (src, ((dst, weight), srcId)) => Try(dst, (srcId, weight)).getOrElse(null)
      }.filter((_: (String, (VertexId, Double))) != null)
    }.partitionBy(partitioner).join(this.node2id, partitioner).mapPartitions {
      (_: Iterator[(String, ((VertexId, Double), VertexId))]).map {
        case (dst, ((srcId, weight), dstId)) => Try(srcId, dstId, weight).getOrElse(null)
      }.filter((_: (VertexId, VertexId, Double)) != null)
    }
  }

  def loadNode2Id(): this.type = {
    try {
      this.node2id = context.textFile(config.nodePath, config.numPartitions).map {
        node2index: String =>
          val Array(strNode, index) = node2index.split("\\s")
          (strNode, index.toLong)
      }
    } catch {
      case _: Exception =>
        logger.info("Failed to read node2index file.")
        this.node2id = null
    }
    this
  }

  def createNode2Id(triplets: RDD[(String, String, Double)]): RDD[(String, VertexId)] = {
    triplets.flatMap { case (src, dst, _) =>
      Try(Array(src, dst)).getOrElse(Array.empty[String])
    }.distinct(config.numPartitions).zipWithIndex()
  }
}
