package service.PACC

import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import service.PACC.StarGroupOps.StarRDDOp
import util.LongExternalSorter
import util.PartImplicitWrapper.CopyOps

import scala.reflect.ClassTag


/**
 * 简化连通图，便于迭代计算
 */
object PACCCompute {
  /**
   * 转换边的关系，不改变图的连通域
   */
  def apply(inputRDD: RDD[(Long,Long)]): RDD[(Long, Long)] = {
    val tmpPaths = inputRDD.sparkContext.hadoopConfiguration.getTrimmedStrings("yarn.nodemanager.local-dirs")
    inputRDD
      .mapPartitions(UnionFind(_))
      .map{
        case (u,v)=>
        val vEnc=v.encode(u.copyId.toInt)
        (vEnc,u)
    }.starGrouped()
      .mapPartitions(
        it => {
          val longExternalSorter = new LongExternalSorter(tmpPaths)
          def processNode(x: (Long, Iterator[Long])): Iterator[(Long, Long)] = {
            val (_u, uN) = x
            val u = _u.nodeId
            var mu = Long.MaxValue
            val _uNStream: Iterator[Long] = uN.map { v =>
              if (mu > v) mu = v
              v
            }
            val uNStream = longExternalSorter.sort(_uNStream)
            uNStream.map { v =>
              if (v != mu) (v, mu)
              else (v, u)
            }
          }
          it.flatMap {processNode}
        }
      ).persist(StorageLevel.DISK_ONLY)
  }


  /**
   * 转换边的，不改变图的连通性
   * @param edges
   * @return
   */
  def apply[ED:ClassTag](edges:RDD[Edge[ED]]):RDD[(Long,Long)]  = {
    val inputRDD = edges.map(edge => (edge.srcId, edge.dstId))
    PACCCompute(inputRDD)
  }
}
