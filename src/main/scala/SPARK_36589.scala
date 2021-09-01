import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.Decimal

object SPARK_36589 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SPARK_36589")
      .master("local[*]")
      .getOrCreate()

   /* val df = spark.read.format("json")
                  .option("multiline", "true")
                  .load("/home/user/work/Parquet/output.json")*/
    println("Example 1 -  " +Decimal(20))
    println("Example 2 -  " +Decimal(20.2))
    println("Example 3 -  " +Decimal(20.4))
    println("Example 4 -  " +Decimal(20.9))
  }

}
