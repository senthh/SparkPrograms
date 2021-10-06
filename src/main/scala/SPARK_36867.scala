import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object SPARK_36867 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Group by issue")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
                    ("2021-09-15", 1),
                    ("2021-09-16", 2),
                    ("2021-09-17", 10),
                    ("2021-09-18", 25),
                    ("2021-09-19", 500),
                    ("2021-09-20", 50),
                    ("2021-09-21", 100),
                    ("2021-09-15", 10),
                    ("2021-09-16", 20),
                    ("2021-09-17", 10),
                    ("2021-09-18", 250),
                    ("2021-09-19", 500),
                    ("2021-09-20", 55),
                    ("2021-09-30", 101)
                  )
    import spark.sqlContext.implicits._
    //val scheme = StructType
    val df = data.toDF("d", "v")
    df.createOrReplaceTempView("data")
    spark.sql("select sum(v) as value, date(date_trunc('week', d)) as week from data group by week").show()
  }

}
