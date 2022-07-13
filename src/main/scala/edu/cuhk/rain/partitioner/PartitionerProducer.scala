package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

trait PartitionerProducer {
  def setup(context: SparkContext): this.type

  def partition(graph: Graph): Partitioner

  def node2id: RDD[(Long, Int)]

  def id2partition: RDD[(Int, Int)]

}
