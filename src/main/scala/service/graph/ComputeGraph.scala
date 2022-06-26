package service.graph

import bean.{ComputeVAttr, IdMsg, VertexAttr, VertexWithMessage}
import conf.oneIDConstant
import org.apache.spark.graphx.{EdgeTriplet, Graph, Pregel, VertexId}
import org.slf4j.{Logger, LoggerFactory}


object ComputeGraph {

  val log: Logger = LoggerFactory.getLogger(this.getClass.getName)
  type InputGraph=Graph[VertexAttr,AnyVal]
  type OutputGraph=Graph[VertexWithMessage,AnyVal]

  def apply(graph: InputGraph): OutputGraph={

    val maxIterations=oneIDConstant.MAX_ITERATIONS

    log.info(s"""
      start graph connected components,
      max iterations:${maxIterations}"""
    )

    doGraphPregel(graph,maxIterations,true)
  }


  protected def doGraphPregel(
                               graph: InputGraph,
                               maxIterations:Int,
                               isClear:Boolean
                             ) ={

    val hyperIdGraph: Graph[VertexWithMessage, AnyVal] = graph.mapVertices {
      case (_, vertex) => VertexWithMessage(vertex)
    }

    // 发送消息
    @inline def sendMsg(edge:EdgeTriplet[VertexWithMessage,AnyVal])={
      val srcHyperId=edge.srcAttr.IdMsg
      val dstHyperId=edge.dstAttr.IdMsg
      val compareResult=srcHyperId.compareTo(dstHyperId)
      if(compareResult<0) Iterator((edge.dstId,srcHyperId))
      else if(compareResult>0) Iterator((edge.srcId,dstHyperId))
      else Iterator.empty
    }

    // 收到消息的处理函数
    @inline def vertexProgram(id:VertexId,attr:VertexWithMessage,msg:IdMsg)={
      val currentHyperId=attr.IdMsg
      if(msg.compareTo(currentHyperId)<0) VertexWithMessage(attr.vertexAttr,msg)
      else VertexWithMessage(attr.vertexAttr,currentHyperId)
    }

    // 合并消息
    @inline def mergeMsg(msg1:IdMsg,msg2:IdMsg):IdMsg={
      if(msg1.compareTo(msg2)<0) msg1 else msg2
    }

    val resultGraph = Pregel[VertexWithMessage,AnyVal,IdMsg](
      hyperIdGraph,IdMsg.DefaultMsg,maxIterations
    )(vertexProgram,sendMsg,mergeMsg)

    hyperIdGraph.unpersist()
    if(isClear) graph.unpersist()
    resultGraph
  }


}
