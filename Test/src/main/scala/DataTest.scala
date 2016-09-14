/**
  * Created by kaiser on 16/8/5.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DataTest {
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      //.master("local[*]")  // Liz delete this for tests on clusters
      .appName("My Spark Application")
      .getOrCreate

    //save as parquets
    val path = saveDataAsParquet(spark)

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
	/*


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
    val savePath: String = "hdfs://dbg11:8020/user/root/test/voice_call_parquet"
    dataDF.write.partitionBy("last_msisdn").parquet(savePath)

    savePath
  }


}
