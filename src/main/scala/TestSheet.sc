import org.apache.spark.{SparkConf, SparkContext}

val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
val sc = new SparkContext(sparkConf)

val rdd1 = sc.makeRDD(Array((1, 2), (1, 3), (2, 4), (2, 5)))
val rdd2 = sc.makeRDD(Array((1, 11), (1, 23), (3, 4), (3, 5)))

rdd1.join(rdd2).collect().foreach(println)