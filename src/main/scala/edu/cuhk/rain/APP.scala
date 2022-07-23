package edu.cuhk.rain

import edu.cuhk.rain.partitioner.PartitionerTester
import edu.cuhk.rain.randomwalk.RandomWalk
import edu.cuhk.rain.util.ParamsPaser
import edu.cuhk.rain.util.ParamsPaser._
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

object APP extends Logging{
  def start(params: Params): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("edu.cuhk.rain.node2vec").setMaster("local[*]")
    val context = new SparkContext(conf)
    context.setLogLevel("warn")
    context.setCheckpointDir("hdfs://~/checkpoint/")

    params.cmd match {
      case Command.node2vec =>
      case Command.randomwalk =>
        RandomWalk.setup(context, params).start()
      case Command.embedding =>
      case Command.partition =>
        PartitionerTester.setup(context, params).partition()
    }
  }

  def test(): Unit = {
    val params: Params = Params(
      partitions = 4,
      weighted = false,
      directed = false,
      //      input = "./data/karate.edgelist",
      input = "./data/BlogCatalog",
      walkLength = 20,
      numWalks = 5,
      cmd = Command.partition
    )
    start(params)
  }

  def main(args: Array[String]): Unit = {
    ParamsPaser.parse(args) match {
      case Some(x) => start(x)
      case _ => logError("error params!")
    }
  }
}
