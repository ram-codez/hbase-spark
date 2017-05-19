package scala

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ram on 5/19/17.
  */
object SparkHBaseReader {

  import org.apache.spark.sql.types.StructType

  val sparkHBaseReader = Logger.getLogger(SparkHBaseReader.getClass)


  def getSchema() : StructType = {
    import org.apache.spark.sql.types.{StringType, StructField}

    val tapad_schema = StructType(Array(
      StructField("rowkey", StringType, false),
      StructField("deviceId", StringType, false),
      StructField("deviceType", StringType, false),
      StructField("firstSeenDate", StringType, false),
      StructField("lastSeenDate", StringType, false)
    ))

    tapad_schema

  }


  def main (args: Array[String]) {
    import org.apache.spark.sql.SQLContext


    Logger.getRootLogger().setLevel(Level.INFO)
    val sparkConf = new SparkConf().setAppName("drill-migration-spark-hbase")
    val sparkCtx = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkCtx)


    import sqlContext.implicits._
    val hBaseConfig = HBaseConfiguration.create()
    hBaseConfig.setInt("timeout", 120000)
    hBaseConfig.set(TableInputFormat.INPUT_TABLE, "myHBaseTable")

    val myHBaseTableRawRDD = sparkCtx.newAPIHadoopRDD(hBaseConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    sparkHBaseReader.debug("###### Tapad HBase records raw count ##### : " + myHBaseTableRawRDD.count())
    val parsedDataMatchRDD = myHBaseTableRawRDD.map(tuple => tuple._2).map(result => (
      Row((result.getRow.map(_.toChar).mkString),
        (result.getColumn("CF".getBytes(),"deviceId".getBytes()).get(0).getValue.map(_.toChar).mkString),
        (result.getColumn("CF".getBytes(),"deviceType".getBytes()).get(0).getValue.map(_.toChar).mkString),
        (result.getColumn("CF".getBytes(),"firstSeenDate".getBytes()).get(0).getValue.map(_.toChar).mkString),
        (result.getColumn("CF".getBytes(),"lastSeenDate".getBytes()).get(0).getValue.map(_.toChar).mkString)
      )))

    val parsedDataMatchDf = sqlContext.createDataFrame(parsedDataMatchRDD,getSchema())
    val filteredDataMatchDf = parsedDataMatchDf.where($"softDelete" === "No")

    sparkHBaseReader.debug("###### Tapad HBase records count after filter ##### : " + filteredDataMatchDf.count())
    filteredDataMatchDf.registerTempTable("sparkHBaseReaderTemp")

    filteredDataMatchDf.printSchema()

    filteredDataMatchDf.write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("delimiter","\t")
      .save("output_path")

    sparkHBaseReader.info("##### query migration task complete #####")
    sparkCtx.stop()


  }

}
