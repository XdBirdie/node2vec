package edu.cuhk.rain.randomwalk

import edu.cuhk.rain.distributed.DistributedSparseMatrix
import edu.cuhk.rain.util.ParamsPaser.Params
import org.apache.spark.SparkContext

case object RandomWalk {
  var context: SparkContext = _
  var config: Params = _

  def setup(context: SparkContext, param: Params): this.type = {
    this.context = context
    this.config = param
    this
  }

  def randomWalk(graph: DistributedSparseMatrix): this.type = {

    this
  }


}
