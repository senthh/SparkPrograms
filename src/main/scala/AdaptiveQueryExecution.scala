import org.apache.spark.sql.SparkSession

object AdaptiveQueryExecution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AdaptiveQueryExecution")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", true)
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)
    spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs",false)
    spark.conf.set("spark.io.encryption.enabled", true)  ///SPARK-34790

    import spark.implicits._

    val simple_data = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    val df = simple_data.toDF("employee_name","department","state","salary","age","bonus")
    //val df1=df.groupBy("department").count()
    //println(df1.rdd.getNumPartitions)
    //df.summary().show()
    //df1.show()
    //val df = spark.table("testData").repartition()
    df.createTempView("testData")
    val df1 = spark.table("testData").repartition('s')
    //df1.show()
    println(df1.rdd.getNumPartitions)
  }

}
