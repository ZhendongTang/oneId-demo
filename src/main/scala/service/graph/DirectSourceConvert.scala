package service.graph

import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import service.graph.DirectSourceConvert.Input

import scala.reflect.ClassTag

/**
 * 将输入IN转化为图
 *
 * @param input  输入
 * @tparam VD    点类型
 * @tparam ED    边类型
 * @tparam CONF  输入配置,必须可序列化
 * @tparam IN    输入类型
 */
class DirectSourceConvert[VD: ClassTag, ED: ClassTag, CONF: ClassTag, IN: ClassTag](
   input: Input[VD, ED, CONF, IN]
 )  {

  /**
   * 默认顶点的属性，默认值为 null
   */
  private var defaultVertexAttr: VD = null.asInstanceOf[VD]

  /**
   * 边的存储级别，默认为 MEMORY_ONLY
   */
  private var edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY

  /**
   * 点的存储级别，默认为 MEMORY_ONLY
   */
  private var vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY

  /**
   * 分区策略，默认为 CanonicalRandomVertexCut
   */
  private var partitionStrategy: PartitionStrategy =
    PartitionStrategy.CanonicalRandomVertexCut

  /**
   * 分区数量，默认为边的分区数量
   */
  private var numPartitions: Option[Int] = None

  def setNumPartitions(
                        numPartitions: Int
                      ): DirectSourceConvert[VD, ED, CONF, IN] = {
    require(Option(numPartitions).nonEmpty)
    this.numPartitions = Some(numPartitions)
    this
  }

  def setPartitionStrategy(
                            partitionStrategy: PartitionStrategy
                          ): DirectSourceConvert[VD, ED, CONF, IN] = {
    require(Option(partitionStrategy).nonEmpty)
    this.partitionStrategy = partitionStrategy
    this
  }

  def setVertexStorageLevel(
                             vertexStorageLevel: StorageLevel
                           ): DirectSourceConvert[VD, ED, CONF, IN] = {
    require(Option(vertexStorageLevel).nonEmpty)
    this.vertexStorageLevel = vertexStorageLevel
    this
  }

  def setEdgeStorageLevel(
                           edgeStorageLevel: StorageLevel
                         ): DirectSourceConvert[VD, ED, CONF, IN] = {
    require(Option(edgeStorageLevel).nonEmpty)
    this.edgeStorageLevel = edgeStorageLevel
    this
  }

  def setDefaultVertexAttr(
                            defaultVertexAttr: VD
                          ): DirectSourceConvert[VD, ED, CONF, IN] = {
    require(Option(defaultVertexAttr).nonEmpty)
    this.defaultVertexAttr = defaultVertexAttr
    this
  }

  /**
   * 转换操作
   *
   * @param input 输入IN
   * @return 输出图
   */
   def convert(
                        input: Input[VD, ED, CONF, IN] = input
                      ): Graph[VD, ED] = {

    val vertexes = input.tranVs(input.in, input.conf)
    val edges = input.tranEs(input.in, input.conf)
    var graph = graphx.Graph(
        vertexes,
        edges,
        defaultVertexAttr,
        edgeStorageLevel,
        vertexStorageLevel
      )
    graph = numPartitions match {
      case None => graph.partitionBy(partitionStrategy)
      case _    => graph.partitionBy(partitionStrategy, numPartitions.get)
    }
    graph
  }
}

object DirectSourceConvert {

  def apply[
    VD: ClassTag,
    ED: ClassTag,
    CONF: ClassTag,
    IN: ClassTag
  ](
     input: Input[VD, ED, CONF, IN]
   ): DirectSourceConvert[VD, ED, CONF, IN] =
    new DirectSourceConvert(input)

  def apply[VD: ClassTag, ED: ClassTag, CONF: ClassTag, IN: ClassTag]
  (input: IN, conf: CONF = null.asInstanceOf[CONF])(
    tranEs: (IN, CONF) => RDD[Edge[ED]],
    tranVs: (IN, CONF) => RDD[(VertexId, VD)]
  ): DirectSourceConvert[VD, ED, CONF, IN] =
    new DirectSourceConvert(Input(input, conf, tranEs, tranVs))

  def apply[VD: ClassTag, ED: ClassTag]()(
    edges: RDD[Edge[ED]], vertexes: RDD[(VertexId, VD)])
  :DirectSourceConvert[VD, ED, Null, Null] = new DirectSourceConvert(
      Input(null, null,
        tranEs = (_, _) => edges,
        tranVs = (_, _) => vertexes
      )
    )

  /**
   * source 输入配置
   *
   * @param in      输入类型
   * @param conf    配置,必须可序列化
   * @param tranEs   边转换
   * @param tranVs   点转换
   * @tparam VD     点类型
   * @tparam ED     边类型
   * @tparam CONF   配置类型,必须可序列化
   * @tparam IN     输入类型
   */
  case class Input[VD: ClassTag, ED: ClassTag, CONF: ClassTag, IN: ClassTag](
     in: IN,
     conf: CONF = null.asInstanceOf[CONF],
     tranEs: (IN, CONF) => RDD[Edge[ED]],
     tranVs: (IN, CONF) => RDD[(VertexId, VD)]
   )
}
