package bean

case class VertexAttr(
                       idType:String,
                       idValue:String,
                       weight:Int
                     ){

  def plus(other:VertexAttr): VertexAttr ={
    VertexAttr(
      idType,idValue,weight+other.weight
    )
  }
}