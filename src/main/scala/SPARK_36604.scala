import org.apache.spark.sql.SparkSession

object SPARK_36604 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SPARK_36604")
      .master("local[*]")
      .getOrCreate()
    spark.sql("CREATE  TABLE IF NOT EXISTS T1(a timestamp) USING HIVE")
    spark.sql("INSERT INTO a SELECT '2021-08-15 15:30:01'")
    spark.sql("SELECT * FROM a")
  }

}
