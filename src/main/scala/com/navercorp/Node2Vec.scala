package com.navercorp

import org.apache.spark.SparkContext

trait Node2Vec extends Serializable {

  def setup(context: SparkContext, param: Main.Params): this.type

  def load(): this.type

  def initTransitionProb(): this.type

  def randomWalk(): this.type

  def embedding(): this.type

  def save(): this.type

  def saveRandomPath(): this.type

  def saveModel(): this.type

  def saveVectors(): this.type

  def loadNode2Id(node2idPath: String): this.type


}
