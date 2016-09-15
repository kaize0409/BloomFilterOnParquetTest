/**
  * Created by lizbai on 15/9/16.
  */

import org.apache.spark.sql.SparkSession

object SimpleLoad {

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
     // .config("spark.sql.parquet.enable.bloom.filter", "true")
     // .config("spark.sql.parquet.bloom.filter.expected.entries", "117015,123718,2,61910,65774")
     // .config("spark.sql.parquet.bloom.filter.col.name", "formatcallerno,callerno,orgcalledno,calledno,formatcalledno")
      //.master("local[*]")  // Liz delete this for tests on clusters
      .appName("Loading Parquet on 2.1")
      .getOrCreate

    //save as parquets
    val path = saveDataAsParquet(spark)
    //spark.sparkContext.stop()
  }
  case class Record(rank:Int, value:Float)

  def saveDataAsParquet(spark: SparkSession): String = {
    import spark.implicits._

    //read data
    val dataDF = spark.sparkContext
      .textFile("hdfs://dbg11:8020/user/root/test/TwoColumns.txt")
      .map(_.split(" "))
      .map(p => Record(p(0).toInt, p(1).toFloat))
      .toDF()

    //save as parquet
    val savePath: String = "hdfs://dbg11:8020/user/root/test/Two_Columns"
    dataDF.write.parquet(savePath)

    savePath
  }
}
