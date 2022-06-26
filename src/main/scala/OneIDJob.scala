import conf.oneIDConstant._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, functions}
import service.Sink
import service.graph.{ComputeGraph, CreateGraph, Df2RawEdge, Df2RawVertex}
import util.{HashUtil, SparkEnv}


object OneIDJob extends SparkEnv{
  private val logger = Logger.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
    val ifUsePACC = args(2).toBoolean
//    val inputPath = "test-data/idmappingResult.txt"
//    val outputPath = "test-data/oneIDResult.txt"
//    val ifUsePACC = true
    val t0 = System.currentTimeMillis()
    val orignalDataDF = readOrignalData(inputPath)
    orignalDataDF.show(50, truncate = false)
    //预处理
    val etlData = preETL(orignalDataDF)
    etlData.show(50, truncate = false)
    //构建边rdd
    val dfToRawEdg =Df2RawEdge(ifUsePACC,0)
    val edgeRdd = dfToRawEdg.toRawEdge(etlData)
    //构建顶点rdd
    val df2RawVertex: Df2RawVertex = Df2RawVertex()
    val vertexRdd = df2RawVertex.toRawVertex(etlData)
    //构建图
    val originalGraph = CreateGraph(edgeRdd, vertexRdd,ifUsePACC)
    val t1 = System.currentTimeMillis()
    //pregel计算
    val computeGraph = ComputeGraph(originalGraph)
    val wideDF = Sink(computeGraph.vertices).getWideDF()
    val t2 = System.currentTimeMillis()
//    wideDF.write
//      .mode(SaveMode.Overwrite)
//      .option("header",true)
//      .csv(outputPath)
    logger.info(s"ifUsePACC: $ifUsePACC")
    logger.info(s"init runs: ${(t1-t0)/1000.0}s")
    logger.info(s"ComputeGraph runs: ${(t2-t1)/1000.0}s")
    wideDF.show(20, truncate = false)
  }

  //读取原始数据
  def readOrignalData(intputPath:String):DataFrame ={
    val orignalData = ss.read.option("delimiter", ";").csv(intputPath)
    orignalData
  }
  //预处理数据
  def preETL(OrignalData:DataFrame):DataFrame={
    val getHidUdf = functions.udf((id_type: String, value: String) => {
      HashUtil.hashToPACCLong(id_type + "::" + value)
    })
    val res = OrignalData.selectExpr(
      s"_c0 as $LEFT_TYPE",
      s"_c1 as $LEFT_VALUE",
      s"10 as $LEFT_WEIGHT",
      s"_c2 as $RIGHT_TYPE",
      s"_c3 as $RIGHT_VALUE",
      s"10 as $RIGHT_WEIGHT",
      s"_c6 as $DATE",
      s"_c8 as $SOURCE",
      s"10 as $WIGHTS")
      .withColumn(LEFT_ID, getHidUdf(col(LEFT_TYPE), col(LEFT_VALUE)))
      .withColumn(RIGHT_ID, getHidUdf(col(RIGHT_TYPE), col(RIGHT_VALUE)))
    res
  }

}
