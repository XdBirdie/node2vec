package edu.cuhk.rain.randomwalk

import edu.cuhk.rain.distributed.{DistributedSparseMatrix, DistributedSparseVector}
import edu.cuhk.rain.graph.Graph
import edu.cuhk.rain.graphBLAS.Semiring
import edu.cuhk.rain.partitioner.{LDGPartitioner, LPTPartitioner, PartitionerProducer}
import edu.cuhk.rain.util.ParamsPaser.{Params, PartitionerVersion}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

case object RandomWalk extends Logging {
  private var context: SparkContext = _
  private var config: Params = _
  private var producer: PartitionerProducer = _

  private var walkPaths: RDD[(Int, Array[Int])] = _

  private var partitioner: Partitioner = _
  private var nodelist: RDD[Int] = _
  private var numNodes: Int = _

  def setup(context: SparkContext, param: Params): this.type = {
    this.context = context
    this.config = param
    this.producer = param.partitioner match {
      case PartitionerVersion.ldg =>
        new LDGPartitioner(config.partitions, context)
      case PartitionerVersion.lpt =>
        new LPTPartitioner(config.partitions, context)
    }
    this
  }

  def start(graph: Graph): this.type = {
    this.producer.partition(graph)
    this.partitioner = producer.partitioner
    logWarning(s"producer.partitioner = $partitioner")

    this.numNodes = producer.numNodes
    val matrix: DistributedSparseMatrix =
      graph.toMatrix(producer.node2id, numNodes).partitionBy(partitioner).cache()
    matrix.count()

    this.nodelist = context.makeRDD(
      Array.range(0, numNodes),
      config.partitions
    ).cache()
    randomWalk(matrix)
    nodelist.unpersist(false)
    this
  }

  private def oneWalk(matrix: DistributedSparseMatrix): RDD[(Int, Array[Int])] = {
    var path: RDD[(Int, ArrayBuffer[Int])] = nodelist.map(
      u => (u, ArrayBuffer(u))
    ).partitionBy(partitioner).cache()
    path.count()

    for (i <- 0 until config.walkLength) {
      logWarning(s"\t step: $i")

      val vector: DistributedSparseVector = DistributedSparseVector.random(numNodes, context)
      val nextWalk: RDD[(Int, Int)] =
        vector.multiply(matrix, Semiring.semiringMaxfracPlus).v.mapValues(_.toInt)

      val tmp: RDD[(Int, Int)] = path.map {
        case (srcId, buffer) => (buffer.last, srcId)
      }.join(nextWalk).map{_._2}

      val lastPath: RDD[(Int, ArrayBuffer[Int])] = path
      path = path.join(tmp, partitioner).mapValues{
        case (buffer, nextHop) => buffer += nextHop; buffer
      }.cache()
      path.count()
      lastPath.unpersist(false)
    }

    val lastPath: RDD[(Int, ArrayBuffer[Int])] = path
    val resPath: RDD[(Int, Array[Int])] = path.mapValues(_.toArray).cache()
    resPath.count()
    lastPath.unpersist(false)
    resPath
  }

  private def randomWalk(matrix: DistributedSparseMatrix): this.type = {
    logWarning("start randomWalk")
    for (i <- 0 until config.numWalks) {
      logWarning(s"walks: $i")
      val onePath: RDD[(Int, Array[Int])] = oneWalk(matrix)
      if (walkPaths == null) {
        walkPaths = onePath.cache()
        walkPaths.count()
      } else {
        val tmp: RDD[(Int, Array[Int])] = walkPaths
        walkPaths = walkPaths.union(onePath).cache()
        walkPaths.count()
        tmp.unpersist(false)
      }
    }
    this
  }

  def save(): this.type = {
    val outputPath: String = config.output
    if (outputPath != null && outputPath.nonEmpty) {
      walkPaths.mapPartitions {
        _.map { case (_, pathBuffer) =>
          Try(pathBuffer.mkString(" ")).getOrElse(null)
        }.filter{_ != null}
      }.repartition(config.partitions).saveAsTextFile(outputPath)
    }
    this
  }

  def debug(): Unit = {
    walkPaths.toLocalIterator.foreach{
      case (u, path) => println(s"u: $u, path: [${path.mkString(", ")}]")
    }
  }
}
