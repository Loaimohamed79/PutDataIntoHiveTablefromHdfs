import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

val spark = SparkSession.builder.appName("ParquetLoader").getOrCreate()
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

val parquetPath = new Path("hdfs://worker1:8020/user/hive/.....................")
val parquetFiles = fs.listStatus(parquetPath).filter(_.getPath.getName.endsWith(".parquet"))

for (file <- parquetFiles) {
  val filePath = file.getPath.toString
  val df = spark.read.parquet(filePath)
  df.createOrReplaceTempView("temp_view")
  spark.sql("INSERT INTO table SELECT * FROM temp_view")
}

spark.stop()

