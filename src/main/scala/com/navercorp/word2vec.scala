package com.navercorp

import org.apache.spark.SparkContext
import org.apache.spark.mllib.embedding.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD

object word2vec extends Serializable {
  var context: SparkContext = _
  var word2vec = new Word2Vec()
  var model: Word2VecModel = _
  private var readRDD: RDD[Iterable[String]] = _
  private var param: Main.Params = _

  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.param = param
    word2vec.setNumIterations(param.iter)
      .setLearningRate(param.lr)
      .setNumPartitions(param.numPartitions)
      .setVectorSize(param.dim)
      .setWindowSize(param.window)
      .setMinCount(param.minCount)

    this
  }

  def read(path: String): RDD[Iterable[String]] = {
    readRDD = context.textFile(path)
      .repartition(param.numPartitions)
      .map((_: String).split("\\s").toIterable)
    readRDD
  }

  def fit(): this.type = fit(readRDD)

  def fit(input: RDD[Iterable[String]]): this.type = {
    model = word2vec.fit(input)
    this
  }

  def save(outputPath: String): this.type = {
    model.save(context, s"$outputPath.bin")
    this
  }

  def load(path: String): this.type = {
    model = Word2VecModel.load(context, path)
    this
  }

  def getVectors: Map[String, Array[Float]] = this.model.getVectors
}
