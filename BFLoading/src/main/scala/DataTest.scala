/**
  * Created by kaiser on 16/8/5.
  */
/*
modified by Liz on 16/9/14
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DataTest {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
        .config("spark.sql.parquet.enable.bloom.filter","true")
        .config("spark.sql.parquet.bloom.filter.expected.entries","117015,123718,2,61910,65774")
        .config("spark.sql.parquet.bloom.filter.col.name","formatcallerno,callerno,orgcalledno,calledno,formatcalledno")
      //.master("local[*]")  // Liz delete this for tests on clusters
      .appName("Loading Parquet with BF")
      .getOrCreate

    //save as parquets
    val path = saveDataAsParquet(spark)

    logger.info("System ready for exit")
    spark.sparkContext.stop()

	/*
    //read parquet file
    val parquetFileDF = spark.read.parquet(path)

    parquetFileDF.printSchema()

    //create temp view
    parquetFileDF.createOrReplaceTempView("VOICE_CALL")

    //execute sql
    val result1 = spark.sql("select * from VOICE_CALL where starttime >= 1433199150 AND " +
      "(FORMATCALLERNO = '20510950' or CALLERNO = '20510950' or ORGCALLEDNO = '20510950' or CALLEDNO = '20510950' or FORMATCALLEDNO = '20510950')" +
      " AND starttime < 1433199200  AND last_msisdn = '0' limit 5000")
	*/


  }

  def saveDataAsParquet(spark: SparkSession): String = {
    import spark.implicits._

    //read data
    val dataDF = spark.sparkContext
      .textFile("hdfs://dbg11:8020/user/root/Final/voicecall")
      .map(_.split("\\|"))
      .map(attributes => new VoiceCall(attributes))
      .toDF()

    //save as parquet
    val savePath: String = "hdfs://dbg11:8020/user/root/test/voice_call_parquet_BF"
    dataDF.write.partitionBy("last_msisdn").parquet(savePath)

    logger.info("SparkSession write task is completed")

    savePath
  }


}
