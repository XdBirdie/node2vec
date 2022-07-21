package edu.cuhk.rain.partitioner

import edu.cuhk.rain.graph.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

trait PartitionerProducer {
  def setup(context: SparkContext): this.type

  def partition(graph: Graph): this.type

  def partitioner: Partitioner

  def node2id: RDD[(Long, Int)]

  def id2partition: RDD[(Int, Int)]

}
