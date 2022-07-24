package edu.cuhk.rain.distributed

import edu.cuhk.rain.graph.Graph
import edu.cuhk.rain.graphBLAS.{BinaryOp, Semiring}
import edu.cuhk.rain.partitioner.LDGPartitioner
import edu.cuhk.rain.util.ParamsPaser.Params
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

case object Distributed {
  var context: SparkContext = _
  var config: Params = _

  def setup(context: SparkContext, param: Params): this.type = {
    this.context = context
    this.config = param
    this
  }

  def testLocal(): Unit = {
    val u = new SparseVector(10, Array(1, 3, 5, 8), Array(0.4, 12, 3.5, 1))
    val v = new SparseVector(10, Array(2, 3, 7, 8), Array(4, 1.3, 0, 1))
    println(u)
    println(v)
    println(u.add(v))
    println(u.multiply(10))
    println(u.multiply(10, BinaryOp.binaryMin))
    println(u.mapActive((u, v) => (u, v)).mkString(", "))
    println(SparseVector.ones(100, Array(1, 3, 30, 43, 56, 99)))
  }

  def start(): Unit = {
    //    testLocal()
//    testCreateMatrix()
    testPipeline()
  }

  def testCreateMatrix(): Unit = {
    val adj: RDD[(Int, Array[(Int, Double)])] = Graph.setup(context, config).fromFile().toAdj
    val value: RDD[(Int, SparseVector)] = adj.map { case (k, a) =>
      (k, new SparseVector(35, a))
    }
    val matrix = new DistributedSparseMatrix(value, 35, 35)
    println(matrix.collect()._1.mkString("\n"))

    val v = new SparseVector(35, Array(4, 5), Array(1, 0.5))
    val vector: DistributedSparseVector = DistributedSparseVector.fromSparseVector(v, context)
    val res: SparseVector = vector.multiply(matrix, Semiring.semiringPlusMin).collect()
    println(res)
  }

  def testPipeline(): Unit = {
    val graph: Graph = Graph.setup(context, config).fromFile()
    val ldg: LDGPartitioner = new LDGPartitioner(config.partitions).partition(graph)
    val partitioner: Partitioner = ldg.partitioner
    val numNodes: Int = ldg.numNodes
    println(s"numNodes = ${numNodes}")
    
    val node2id: RDD[(Int, Int)] = ldg.node2id
    
    val matrix: DistributedSparseMatrix = graph.toMatrix(node2id).partitionBy(partitioner)
    matrix.rows.count()
  }
}
