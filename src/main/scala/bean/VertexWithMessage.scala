package bean


case class VertexWithMessage(
                          vertexAttr: VertexAttr,
                          IdMsg: IdMsg
                        )

object VertexWithMessage{

  def apply(vertex: VertexAttr) : VertexWithMessage={
    VertexWithMessage(vertex, IdMsg(vertex))
  }

}

