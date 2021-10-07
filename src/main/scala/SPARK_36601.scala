import org.apache.spark.sql.SparkSession

object SPARK_36601 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SPARK_36601")
      .master("local[*]")
      .getOrCreate()
    spark.read.option("multiLine", true).json("/Users/senthilkumar/Documents/SenthilKumar/test.json").show

    // FIXED
  }
}
