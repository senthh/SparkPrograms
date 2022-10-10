import org.apache.spark.sql.SparkSession
//ref : https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
object SPARK_36758_nullable_values {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SPARK_36758_nullable_values")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.format("jdbc")
      .option("database","Test_DB")
      .option("user", "root")
      .option("password", "")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/Test_DB")
      .option("dbtable", "timestamp_test")
      .load()
    df.printSchema()
    //df.show()

  }

}
