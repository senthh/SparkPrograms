import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}

object SPARK_35563 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SPARK_35563")
      .master("local[*]")
      .getOrCreate()
    val windowSpec = Window.partitionBy("a").orderBy("b")

    val df = spark.range(Int.MaxValue.toLong + 100).selectExpr(s"1 as a", "id as b")

    spark.time(df.select(col("a"), col("b"), row_number().over(windowSpec).alias("rn")).orderBy(desc("a"), desc("b")).select((col("rn") < 0).alias("dir")).groupBy("dir").count.show(20))
  }
}
