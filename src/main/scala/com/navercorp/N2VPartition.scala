package com.navercorp


import java.io.Serializable
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{EdgeTriplet, Graph, _}
import com.navercorp.graph.{EdgeAttr, GraphOps, NodeAttr}
import com.navercorp.util.TimeRecorder

object N2VPartition extends Node2Vec {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName);
  
  var context: SparkContext = null

  var config: Main.Params = null
  var node2id: RDD[(String, Long)] = null
  var indexedEdges: RDD[Edge[EdgeAttr]] = _
  var indexedNodes: RDD[(VertexId, NodeAttr)] = _
  var graph: Graph[NodeAttr, EdgeAttr] = _
  var randomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = null

  def setup(context: SparkContext, param: Main.Params): this.type = {
    logger.warn("N2VPartition setup")

    this.context = context
    this.config = param
    this
  }
  
  def load(): this.type = {
    logger.warn("N2VPartition load")

    val bcMaxDegree = context.broadcast(config.degree)
    val bcEdgeCreator = config.directed match {
      case true => context.broadcast(GraphOps.createDirectedEdge)
      case false => context.broadcast(GraphOps.createUndirectedEdge)
    }
    
    val inputTriplets: RDD[(Long, Long, Double)] = config.indexed match {
      case true => readIndexedGraph(config.input)
      case false => indexingGraph(config.input)
    }
    
    indexedNodes = inputTriplets.flatMap { case (srcId, dstId, weight) =>
      bcEdgeCreator.value.apply(srcId, dstId, weight)
    }.reduceByKey(_++_).map { case (nodeId, neighbors: Array[(VertexId, Double)]) =>
      var neighbors_ = neighbors
      if (neighbors_.length > bcMaxDegree.value) {
        neighbors_ = neighbors.sortWith{ case (left, right) => left._2 > right._2 }.slice(0, bcMaxDegree.value)
      }
        
      (nodeId, NodeAttr(neighbors = neighbors_.distinct))
    }.repartition(config.numPartition).cache
    
    indexedEdges = indexedNodes.flatMap { case (srcId, clickNode) =>
      clickNode.neighbors.map { case (dstId, weight) =>
          Edge(srcId, dstId, EdgeAttr())
      }
    }.repartition(config.numPartition).cache
    
    this
  }
  

  def initTransitionProb(): this.type = {
    logger.warn("N2VPartition init transition prob")

    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    
    graph = Graph(indexedNodes, indexedEdges)
            .mapVertices[NodeAttr] { case (vertexId, clickNode) =>
              val (j, q) = GraphOps.setupAlias(clickNode.neighbors)
              val nextNodeIndex: Int = GraphOps.drawAlias(j, q)
              clickNode.path = Array(vertexId, clickNode.neighbors(nextNodeIndex)._1)
              
              clickNode
            }.cache()
            .mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
              val (j, q) = GraphOps.setupEdgeAlias(bcP.value, bcQ.value)(edgeTriplet.srcId, edgeTriplet.srcAttr.neighbors, edgeTriplet.dstAttr.neighbors)
              edgeTriplet.attr.J = j
              edgeTriplet.attr.q = q
              edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1)
              
              edgeTriplet.attr
            }.cache()
    this
  }
  
  def randomWalk(): this.type = {
    logger.warn("N2VPartition random walk")

    val partitioner = new HashPartitioner(config.numPartition)

    val edge2attr: RDD[((VertexId, VertexId), EdgeAttr)] = graph.triplets.map { 
      edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
      ((edgeTriplet.srcId, edgeTriplet.dstId), edgeTriplet.attr)
    }.partitionBy(partitioner).cache()
    edge2attr.count()
    logger.warn("N2VPartition edge2attr set up")

    for (iter <- 0 until config.numWalks) {
      logger.warn(s"N2VPartition begin random walk ${iter}")

      var prevWalk: RDD[(Long, ArrayBuffer[Long])] = null
      var randomWalk: RDD[(VertexId, ArrayBuffer[VertexId])] = graph.vertices.map { case (nodeId, clickNode) =>
        val pathBuffer = new ArrayBuffer[Long]()
        pathBuffer.append(clickNode.path:_*)
        (nodeId, pathBuffer)
      }.cache
      randomWalk.count()

      for (walkCount <- 0 until config.walkLength) {
        prevWalk = randomWalk
        val tempWalk = randomWalk.map { case (srcNodeId, pathBuffer) =>
          val prevNodeId: VertexId = pathBuffer(pathBuffer.length - 2)
          val currentNodeId: VertexId = pathBuffer.last

          ((prevNodeId, currentNodeId), (srcNodeId, pathBuffer))
        }.partitionBy(partitioner)

        randomWalk = edge2attr.join(tempWalk, partitioner).map { case (edge, (attr, (srcNodeId, pathBuffer))) =>
          val nextNodeIndex: Int = GraphOps.drawAlias(attr.J, attr.q)
          val nextNodeId: VertexId = attr.dstNeighbors(nextNodeIndex)
          pathBuffer.append(nextNodeId)

          (srcNodeId, pathBuffer)
        }.cache()

        randomWalk.count()
        prevWalk.unpersist(blocking=false)
      }

      if (randomWalkPaths != null) {
        val prevRandomWalkPaths: RDD[(VertexId, ArrayBuffer[VertexId])] = randomWalkPaths
        randomWalkPaths = randomWalkPaths.union(randomWalk).cache()
        randomWalkPaths.count()
        prevRandomWalkPaths.unpersist(blocking = false)
      } else {
        randomWalkPaths = randomWalk
      }
    }
    logger.warn(s"End random walk")
    this
  }
  
  def embedding(): this.type = {
    logger.warn(s"N2VPartition begin embedding")

    val randomPaths = randomWalkPaths.map { case (vertexId, pathBuffer) =>
      Try(pathBuffer.map(_.toString).toIterable).getOrElse(null)
    }.filter(_!=null)
    
    Word2vec.setup(context, config).fit(randomPaths)

    logger.warn("N2VPartition end embedding")
    this
  }
  
  def save(): this.type = {
    this.saveRandomPath()
        .saveModel()
        .saveVectors()
  }
  
  def saveRandomPath(): this.type = {
    randomWalkPaths
            .map { case (vertexId, pathBuffer) =>
              Try(pathBuffer.mkString("\t")).getOrElse(null)
            }
            .filter(x => x != null && x.replaceAll("\\s", "").length > 0)
            .repartition(config.numPartition)
            .saveAsTextFile(config.output)
    
    this
  }
  
  def saveModel(): this.type = {
    Word2vec.save(config.output)
    
    this
  }
  
  def saveVectors(): this.type = {
    val node2vector = context.parallelize(Word2vec.getVectors.toList)
            .map { case (nodeId, vector) =>
              (nodeId.toLong, vector.mkString(","))
            }
    
    if (this.node2id != null) {
      val id2Node = this.node2id.map{ case (strNode, index) =>
        (index, strNode)
      }
      
      node2vector.join(id2Node)
              .map { case (nodeId, (vector, name)) => s"$name\t$vector" }
              .repartition(config.numPartition)
              .saveAsTextFile(s"${config.output}.emb")
    } else {
      node2vector.map { case (nodeId, vector) => s"$nodeId\t$vector" }
              .repartition(config.numPartition)
              .saveAsTextFile(s"${config.output}.emb")
    }
    
    this
  }
  
  def cleanup(): this.type = {
    node2id.unpersist(blocking = false)
    indexedEdges.unpersist(blocking = false)
    indexedNodes.unpersist(blocking = false)
    graph.unpersist(blocking = false)
    randomWalkPaths.unpersist(blocking = false)
    
    this
  }

  def loadNode2Id(node2idPath: String): this.type = {
    try {
      this.node2id = context.textFile(config.nodePath).map { node2index =>
        val Array(strNode, index) = node2index.split("\\s")
        (strNode, index.toLong)
      }
    } catch {
      case e: Exception => logger.info("Failed to read node2index file.")
      this.node2id = null
    }

    this
  }
  
  def readIndexedGraph(tripletPath: String) = {
    val bcWeighted = context.broadcast(config.weighted)
    
    val rawTriplets = context.textFile(tripletPath)
    if (config.nodePath == null) {
      this.node2id = createNode2Id(rawTriplets.map { triplet =>
        val parts = triplet.split("\\s")
        (parts.head, parts(1), -1)
      })
    } else {
      loadNode2Id(config.nodePath)
    }

    rawTriplets.map { triplet =>
      val parts = triplet.split("\\s")
      val weight = bcWeighted.value match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }

      (parts.head.toLong, parts(1).toLong, weight)
    }
  }
  

  def indexingGraph(rawTripletPath: String): RDD[(Long, Long, Double)] = {
    val rawEdges = context.textFile(rawTripletPath).map { triplet =>
      val parts = triplet.split("\\s")

      Try {
        (parts.head, parts(1), Try(parts.last.toDouble).getOrElse(1.0))
      }.getOrElse(null)
    }.filter(_!=null)
    
    this.node2id = createNode2Id(rawEdges)
    
    rawEdges.map { case (src, dst, weight) =>
      (src, (dst, weight))
    }.join(node2id).map { case (src, (edge: (String, Double), srcIndex: Long)) =>
      try {
        val (dst: String, weight: Double) = edge
        (dst, (srcIndex, weight))
      } catch {
        case e: Exception => null
      }
    }.filter(_!=null).join(node2id).map { case (dst, (edge: (Long, Double), dstIndex: Long)) =>
      try {
        val (srcIndex, weight) = edge
        (srcIndex, dstIndex, weight)
      } catch {
        case e: Exception => null
      }
    }.filter(_!=null)
  }
  
  def createNode2Id[T <: Any](triplets: RDD[(String, String, T)]): RDD[(String, VertexId)] = triplets.flatMap { case (src, dst, weight) =>
    Try(Array(src, dst)).getOrElse(Array.empty[String])
  }.distinct().zipWithIndex()

}
