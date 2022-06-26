package service.graph

import bean.{ComputeVAttr, VertexAttr}
import org.apache.spark.graphx.PartitionStrategy.CanonicalRandomVertexCut
import org.apache.spark.graphx.{Edge, PartitionID, PartitionStrategy, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import com.google.common.collect.HashBiMap
import conf.oneIDConstant.PARTITION_NUMBER
import util.CompressionUtils
import util.PartImplicitWrapper.CopyOps

object CreateGraph {

  type EdgeRDD = RDD[(Long, Long)]
  type VertexRDD = RDD[(VertexId, VertexAttr)]

  def apply(edgeRDD: EdgeRDD, vertexRDD: VertexRDD,usePACC:Boolean) = {
    val direct = DirectSourceConvert()(toEdge(edgeRDD), vertexRDD)
    direct
      .setPartitionStrategy(if (usePACC) PartitionStrategyPACC else CanonicalRandomVertexCut)
      .setNumPartitions(PARTITION_NUMBER)
      .setVertexStorageLevel(StorageLevel.DISK_ONLY)
      .setEdgeStorageLevel(StorageLevel.DISK_ONLY)
      .convert()
  }

  //转化边类型 (LONG,LONG) => Edge
  def toEdge(input: EdgeRDD): RDD[Edge[AnyVal]] = {
    input.map(edge => Edge(edge._1, edge._2))
  }

  //转化顶点结构
  def toVertex(input: VertexRDD, typeMap: HashBiMap[Int, String]): RDD[(VertexId, ComputeVAttr)] = {
    val startType = 1
    val endType = 10
    val set = (value: String) => CompressionUtils.setValue(typeMap, startType, endType)(value, 0)
    input.map {
      case (id, attr) =>
        id -> ComputeVAttr(attr.idValue, set(attr.idType))
    }
  }

  case object PartitionStrategyPACC extends PartitionStrategy{
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = src.mod(numParts)
  }
}