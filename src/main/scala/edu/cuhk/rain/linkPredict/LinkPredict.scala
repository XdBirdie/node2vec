package edu.cuhk.rain.linkPredict

import edu.cuhk.rain.util.ParamsPaser.Params
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case object LinkPredict {
  var sc: SparkContext = _
  var embeddingPath = ""
  var edgeListPath = ""
  var numPartition = 64
  var directed = false
  var logger: Logger = LoggerFactory.getLogger(LinkPredict.getClass)

  def negativeSample(edgeList: Array[(Long, Long)], nodeList: Array[Long], ratio: Double): Array[(Long, Long)] = {
    logger.warn("begin negative sample!")
    val graph: Map[Long, ArrayBuffer[Long]] = nodeList.map((_: Long, ArrayBuffer[Long]())).toMap
    edgeList.foreach { case (u, v) => graph(u).append(v) }
    logger.warn("calculate graph over!")
    val nodesNum: Int = nodeList.length
    var cnt = 0
    val samples: Map[Long, Array[Long]] = graph.map { case (u, neighbors) =>
      val n: Int = (ratio * neighbors.length).toInt
      var samples: Array[Long] = new Array[Long](n + neighbors.length).map {
        (_: Long) => nodeList(Random.nextInt(nodesNum))
      }
      samples = samples.distinct.diff(neighbors).distinct.take(n)
      cnt += 1
      if (cnt % 100000 == 0) {
        logger.warn(s"$cnt: ($u, ${neighbors.length}, ${n}, ${samples.length}\n[${neighbors.mkString(", ")}]\n[${samples.mkString(", ")}])")
      }
      (u, samples)
    }
    logger.warn("sample over!")
    samples.flatMap { case (u, vs) => vs.map((u, _: Long)) }.toArray
  }

  def negSampleByPair(edgeList: Array[(Long, Long)], nodeList: Array[Long], ratio: Double): Array[(Long, Long)] = {
    val edgeSet: Set[(Long, Long)] = edgeList.toSet
    val negativeSet = new mutable.HashSet[(Long, Long)]()
    val n: Double = edgeList.length * ratio
    val nodesNum: Int = nodeList.length
    while (negativeSet.size < n) {
      val u: Long = nodeList(Random.nextInt(nodesNum))
      val v: Long = nodeList(Random.nextInt(nodesNum))
      if (!edgeSet.contains((u, v))) {
        negativeSet += Tuple2(u, v)
      }
    }
    negativeSet.toArray
  }

  def edgeEmbedding(edgeList: Array[(Long, Long)], embedding: Array[(Long, Array[Double])]): Array[Double] = {
    val map: Map[Long, Array[Double]] = embedding.toMap
    edgeList.map { case (u, v) =>
      if (map.contains(u) && map.contains(v)) {
        val vec1: Array[Double] = map(u)
        val vec2: Array[Double] = map(v)
        val len1: Double = math.sqrt(vec1.map(x => x * x).sum)
        val len2: Double = math.sqrt(vec2.map(x => x * x).sum)
        vec1.zip(vec2).map { case (x1, x2) => x1 * x2 }.sum / len1 / len2
      } else Double.NaN
    }.filter(_ != Double.NaN)
  }

  def getNodeList(edgeList: Array[(Long, Long)]): Array[Long] = {
    edgeList.flatMap((x: (Long, Long)) => Array(x._1, x._2)).distinct
  }

  def setup(context: SparkContext, param: Params): this.type = {
    this.sc = context
    this.numPartition = param.partitions
    this.directed = param.directed
    this.edgeListPath = s"graph/${param.input}"
    this.embeddingPath = s"emb/${param.input}-One.emb"
    this
  }

  def calAUC(dataset: Array[(Double, Int)]): Double = {
    val totalNum: Long = dataset.length
    val posNum: Long = dataset.count(_._2 > 0)
    val negNum: Long = totalNum - posNum

    logger.warn(s"posNum: $posNum, negNum: $negNum, product: ${posNum.toLong * negNum}")
    var negSum: Long = 0
    var posGTNeg: Long = 0
    dataset.sortBy(_._1).foreach { case (_, label) =>
      if (label == 1) posGTNeg += negSum
      else negSum += 1
    }

    logger.warn(s"negSum: $negSum, posGTNeg: $posGTNeg")
    posGTNeg.toDouble / posNum.toDouble / negNum.toDouble
  }

  def calAUC2(dataset: Array[(Double, Int)]): Double = {
    val totalNum: Long = dataset.length
    val posNum: Long = dataset.count(_._2 > 0)
    val negNum: Long = totalNum - posNum

    val sum: Long = dataset.sortBy(_._1).zipWithIndex.map { case ((_, label), rnk) =>
      if (label == 1) rnk + 1L else 0L
    }.sum
    (sum - posNum * (posNum + 1) / 2).toDouble / (posNum * negNum)
  }

  def run(): Unit = {
    logger.warn("run!")
    val directedBC: Broadcast[Boolean] = sc.broadcast(directed)

    val embedding: Array[(Long, Array[Double])] = sc.textFile(embeddingPath, numPartition).mapPartitions {
      (_: Iterator[String]).map { line: String =>
        val ss: Array[String] = line.split("\\s")
        val key: Long = ss.head.toLong
        val value: Array[Double] = ss.tail.map((_: String).toDouble)
        (key, value)
      }
    }.collect()
    logger.warn(s"collect embedding over! size: ${embedding.length}")

    val edgeList: Array[(Long, Long)] = sc.textFile(edgeListPath, numPartition).mapPartitions {
      (_: Iterator[String]).flatMap { line: String =>
        val ss: Array[String] = line.split("\\s")
        val u: Long = ss(0).toLong
        val v: Long = ss(1).toLong
        if (directedBC.value) Array((u, v))
        else Array((u, v), (v, u))
      }
    }.collect()
    logger.warn("collect edge list over!")

    val nodeList: Array[Long] = getNodeList(edgeList)
    logger.warn("calculate node list over!")

    val negativeEdge: Array[(Long, Long)] = negativeSample(edgeList, nodeList, 1)
    logger.warn(s"negative edge get! \nneg size: ${negativeEdge.length}, edge size: ${edgeList.length}")

    val posSamples: Array[Double] = edgeEmbedding(edgeList, embedding)
    val negSamples: Array[Double] = edgeEmbedding(negativeEdge, embedding)
    val scores: Array[Double] = posSamples ++ negSamples
    val truths: Array[Int] = Array.fill(posSamples.length)(1) ++ Array.fill(negSamples.length)(0)
    logger.warn(s"scores get! size: ${scores.length}")
    logger.warn(s"Area under ROC 2 = ${calAUC2(scores.zip(truths))}")
  }
}
