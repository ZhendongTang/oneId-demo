package service

import bean.{SinkAttr, VertexWithMessage}
import conf.oneIDConstant.{GENERATE_DATE, ID_MAP, ONE_ID}
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.slf4j.{Logger, LoggerFactory}
import util.HashUtil.str2Md5
import util.SparkEnv

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.{HashMap => mHashMap, HashSet => mHashSet, Map => mMap, Set => mSet}

class Sink(vertRDD: VertexRDD[VertexWithMessage]) extends SparkEnv with Serializable {
  val log: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def getWideDF(): DataFrame= {
    import ss.implicits._
    val sinkRDD=
    vertRDD.map(vertex=>{
      val VertexWithMessage = vertex._2
      val vertexAttr = VertexWithMessage.vertexAttr
      val message = VertexWithMessage.IdMsg
      SinkAttr(
        str2Md5(message.idType+"::"+message.idValue),
        vertexAttr.idType,
        vertexAttr.idValue
      )
    })
    val wideDF=sinkRDD
      .map(sinkAttr => sinkAttr.oneId -> sinkAttr)
      .aggregateByKey[mMap[String,mSet[String]]](
        new mHashMap[String, mSet[String]]
      )(putSinkAttr2Map,merge2Map)
      .mapValues(map=> {
        map.mapValues(values => values.toSeq).toMap
      })
      .toDF(ONE_ID, ID_MAP)
      .withColumn(GENERATE_DATE,lit(getNow))
      .selectExpr(ONE_ID,ID_MAP,GENERATE_DATE)
    wideDF
  }

  def putSinkAttr2Map(map:mMap[String,mSet[String]],sinkAttr:SinkAttr)={
    val valueSet = map.getOrElse(sinkAttr.idType, new mHashSet[String])
    valueSet.add(sinkAttr.idValue)
    map.put(sinkAttr.idType,valueSet)
    map
  }

  def merge2Map(map1:mMap[String,mSet[String]],map2:mMap[String,mSet[String]])={
    map2.foreach(entry=>{
      val (idType,idValueSet)=entry
      val set=map1.getOrElse(idType,new mHashSet[String])
      idValueSet.foreach(idValue=>set.add(idValue))
      map1.put(idType,set)
    })
    map1
  }

  def getNow: String={
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val cal: Calendar = Calendar.getInstance()
    val now = dateFormat.format(cal.getTime)
    now
  }
}

object Sink {
  def apply(vertRDD: VertexRDD[VertexWithMessage]): Sink =
    new Sink(vertRDD)
}