package edu.cuhk.rain.util

import edu.cuhk.rain.util.ParamsPaser.Params
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

case object DataLoader {
  var context: SparkContext = _
  var config: Params = _
  def setup(spark: SparkSession, param: Params): this.type = {
    this.context = spark.sparkContext
    this.config = param
    this
  }

  def loadTriplet(rawTripletPath: String=config.input): RDD[(Long, Long, Double)] = {
    val bcWeighted: Broadcast[Boolean] = context.broadcast(config.weighted)
    context.textFile(rawTripletPath, config.partitions).mapPartitions {_.map {
      triplet: String =>
        val parts: Array[String] = triplet.split("\\s")
        val weight: Double = if (bcWeighted.value) Try(parts.last.toDouble).getOrElse(1.0) else 1.0
        (parts.head.toLong, parts(1).toLong, weight)
    }}
  }

  def indexGraph(edgeList: RDD[(Long, Long, Double)], node2id: RDD[(Long, Long)]): Unit = {

  }

  def loadOrCreateNode2Id(): Unit = {

  }


}
