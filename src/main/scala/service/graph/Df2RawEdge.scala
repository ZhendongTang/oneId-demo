package service.graph

import conf.oneIDConstant._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class Df2RawEdge(USE_PACC: Boolean, threshold: Double) extends Serializable {

  private[this] val SRC_ID = "src_id"
  private[this] val DST_ID = "dst_id"
  private[this] val SUM_WIGHTS = "sum_wights"

   def toRawEdge(df: DataFrame): RDD[(Long, Long)] = {

    var raw = filterThreshold(df)
      .rdd
      .map(row => row.getLong(0) -> row.getLong(1))
    if (USE_PACC) {
      if (raw.getNumPartitions != PARTITION_NUMBER) raw = raw.repartition(PARTITION_NUMBER)
//      val res = PACC(raw)
      val res = raw
      res
    }
    else raw
  }

  //Threshold filtering
  private[this] def filterThreshold(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    df
      .selectExpr(
        s"if( $LEFT_ID < $RIGHT_ID ,$LEFT_ID,$RIGHT_ID ) as $SRC_ID",
        s"if( $LEFT_ID < $RIGHT_ID ,$RIGHT_ID,$LEFT_ID ) as $DST_ID",
        s"$WIGHTS")
      .groupBy(SRC_ID, DST_ID)
      .agg(sum(WIGHTS).alias(SUM_WIGHTS))
      .filter(s"$SUM_WIGHTS >= $threshold")
      .select(SRC_ID, DST_ID)
  }
}

object Df2RawEdge {

  def apply(USE_PACC: Boolean, threshold: Double): Df2RawEdge =
    new Df2RawEdge(USE_PACC, threshold)
}