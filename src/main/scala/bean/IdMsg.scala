package bean

case class IdMsg(
                       idType:String,
                       idValue: String,
                       idWeight: Int
                     ){

  def compareTo(other:IdMsg)={
    val priorityCompareResult=idWeight.compareTo(other.idWeight)
    if(priorityCompareResult!=0){
      priorityCompareResult
    }else{
      idValue.compareTo(other.idValue)
    }
  }
}

object IdMsg{

  def apply(vertex: VertexAttr): IdMsg={
    apply(vertex.idType,vertex.idValue,vertex.weight)
  }
  val DefaultMsg=IdMsg("unkownIdType","unkownIdValue",Int.MaxValue)

}
