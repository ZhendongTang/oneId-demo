package service.graph

import bean.VertexAttr
import conf.oneIDConstant._
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame



class Df2RawVertex extends Serializable {

   def toRawVertex(df: DataFrame): RDD[(VertexId, VertexAttr)] = {
    val ss = df.sparkSession
    import ss.implicits._
    df
      .select(
        LEFT_ID, LEFT_TYPE, LEFT_VALUE, LEFT_WEIGHT,
        RIGHT_ID, RIGHT_TYPE, RIGHT_VALUE, RIGHT_WEIGHT
      )
      .flatMap(row => {
        val leftId = row.getAs[Long](LEFT_ID)
        val leftIdType = row.getAs[String](LEFT_TYPE)
        val leftIdValue = row.getAs[String](LEFT_VALUE)
        val leftIdPriority = row.getAs[Int](LEFT_WEIGHT)

        val rightId = row.getAs[Long](RIGHT_ID)
        val rightIdType = row.getAs[String](RIGHT_TYPE)
        val rightIdValue = row.getAs[String](RIGHT_VALUE)
        val rightIdPriority = row.getAs[Int](RIGHT_WEIGHT)

        val leftVertex = VertexAttr(
         leftIdType, leftIdValue, leftIdPriority)
        val rightVertex = VertexAttr(
          rightIdType, rightIdValue, rightIdPriority)

        Seq(leftId -> leftVertex, rightId -> rightVertex)
      })
      .rdd
      .reduceByKey(_ plus _)
  }

}
object Df2RawVertex {
  def apply(): Df2RawVertex = new Df2RawVertex()
}