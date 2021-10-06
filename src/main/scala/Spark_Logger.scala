import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, LogManager, Logger}

object Spark_Logger {
  def main(args: Array[String]): Unit = {
    //val logger: Logger = Logger.getLogger("Spark_Logger")
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    log.info("Hello Logger")
    val spark = SparkSession.builder()
      .appName("Spark_Logger")
      .master("local[*]")
      .getOrCreate()
    log.info("Ending Logger")

    val data = Seq(
                    ("2021-01-01T00", 0),
                    ("2021-01-01T01", 1),
                    ("2021-01-01T02", 2)
                  )
    import spark.sqlContext.implicits._
    val df = data.toDF("hour", "i")

    df.write.partitionBy("hour").parquet("/tmp/t2")
    spark.read.parquet("/tmp/t1").printSchema()





  }

}
