package edu.cuhk.rain.randomwalk

import edu.cuhk.rain.distributed.{DistributedSparseMatrix, DistributedSparseVector}
import edu.cuhk.rain.graph.Graph
import edu.cuhk.rain.graphBLAS.Semiring
import edu.cuhk.rain.partitioner.LDGPartitioner
import edu.cuhk.rain.util.ParamsPaser.{Params, parse}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import scala.collection.mutable.ArrayBuffer

case object RandomWalk extends Logging{
  private var context: SparkContext = _
  private var config: Params = _

  private var walkPath: RDD[(Int, Array[Int])] = _

  private var partitioner: Partitioner = _
  private var nodelist: RDD[Int] = _
  private var numNodes: Int = _

  def setup(context: SparkContext, param: Params): this.type = {
    this.context = context
    this.config = param
    this
  }

  def start(): this.type = {
    val graph: Graph = Graph.setup(context, config).fromFile()
    nodelist = graph.nodelist.cache()

    val ldg: LDGPartitioner = new LDGPartitioner(config.partitions).partition(graph)
    partitioner = ldg.partitioner
    numNodes = ldg.numNodes
    println(s"numNodes = ${numNodes}")
    val node2id: Array[(Int, Int)] = ldg.node2id

    val matrix: DistributedSparseMatrix =
      graph.toMatrix(node2id, numNodes).partitionBy(partitioner).cache()
    matrix.count()

    randomWalk(matrix)
  }

  def oneWalk(matrix: DistributedSparseMatrix): RDD[(Int, Array[Int])] = {
    var path: RDD[(Int, ArrayBuffer[Int])] =
      nodelist.map(u => (u, ArrayBuffer(u))).partitionBy(partitioner).cache()
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
        case (buffer, nextHop) => buffer.append(nextHop); buffer
      }.cache()
      path.count()
      lastPath.unpersist()
    }
    val lastPath: RDD[(Int, ArrayBuffer[Int])] = path
    val resPath: RDD[(Int, Array[Int])] = path.mapValues(_.toArray).cache()
    resPath.count()
    lastPath.unpersist()
    resPath
  }

  def randomWalk(matrix: DistributedSparseMatrix): this.type = {
    logWarning("start randomWalk")
    for (i <- 0 until config.numWalks) {
      logWarning(s"walks: $i")
      val onePath: RDD[(Int, Array[Int])] = oneWalk(matrix)
      if (walkPath == null) {
        walkPath = onePath.cache()
        walkPath.count()
      } else {
        val tmp: RDD[(Int, Array[Int])] = walkPath
        walkPath = walkPath.union(onePath).cache()
        walkPath.count()
        tmp.unpersist()
      }
    }
    this
  }

  def debug(): Unit = {
    walkPath.toLocalIterator.foreach{
      case (u, path) => println(s"u: $u, path: [${path.mkString(", ")}]")
    }
  }
}
