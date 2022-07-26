package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import scala.reflect.ClassTag

trait PartitionerProducer extends Logging with Serializable {
  val numPartition: Int

  private var context: SparkContext = _

  def setup(context: SparkContext): this.type = {
    this.context = context
    this
  }

  def partition(graph: Graph): this.type

  def partitioner: Partitioner

  def node2id: RDD[(Int, Int)]

  def node2partition: Map[Int, Int]

  protected def node2id[T <: Partition](
                   partitions: Array[T],
                   thresholds: Array[Int],
                   numPartition: Int
                 )(implicit ct: ClassTag[T]): RDD[(Int, Int)] = {
    val bcThresholds: Broadcast[Array[Int]] = context.broadcast(thresholds)

    val res: RDD[(Int, Int)] = context.makeRDD(partitions, numPartition).mapPartitions {
      _.flatMap{ partition =>
        var id: Int = bcThresholds.value(partition.id)
        partition.mapNodes { node =>
          val res: (Int, Int) = (node, id)
          id += 1
          res
        }
      }
    }

    bcThresholds.unpersist(false)
    res
  }

  def numNodes: Int

}
