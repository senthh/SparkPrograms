import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CatalystOptimizer_Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CatalystOptimizer_Demo")
      .master("local[*]")
      .getOrCreate()

    val df1 = spark.range(10).withColumn("x*2", col("id") * 2)
    df1.printSchema()

    val df2 = spark.range(10).withColumn("x*3", col("id") * 3)
    df2.printSchema()

    val joinDF = df1.join(df2, df1.col("id") === df2.col("id"))

    joinDF.explain(true)

    joinDF.show()

  }

}
