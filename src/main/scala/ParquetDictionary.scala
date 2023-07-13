import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

// import scala.Seq

object ParquetDictionary {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parquet Dictionary")
      .master("local[*]")
      .getOrCreate()
    /**
    val tableSchema = StructType(List(
      ("id","int"),("name","string"),("percentage","double")
    ));
    spark.sqlContext.createDataFrame(
      rowRDD = spark.sparkContext.parallelize(
        Seq(Row(101, "alex",88.56),
          Row(102, "john",68.32),
          Row(103, "peter",75.62),
          Row(104, "jeff",92.67),
          Row(105, "mathew",89.56),
          Row(106, "alan",72.57),
          Row(107, "steve",96.12),
          Row(108, "mark",98.45),
          Row(109, "adam",76.25),
          Row(109, "david",78.45)),
        schema = tableSchema));
    */
    val col1 = "id1"
    val col2 = "id2"
    val col3 = "id3"

    val table1schema = StructType(List(
      StructField(col1, IntegerType),
      StructField(col2, DoubleType),
      StructField(col3, IntegerType)
    ));
    val table1 = spark.sqlContext.createDataFrame(
      rowRDD = spark.sparkContext.parallelize(Seq(
        Row(1, 2.0,3)
      )),
      schema = table1schema
    )
    table1.printSchema()
    table1.show()
    table1.write.parquet("/Users/senthilkumar/Documents/SenthilKumar/Programs/ParquetwithoutDict")
    table1.write.option("parquet.filter.dictionary.enabled",true)parquet("/Users/senthilkumar/Documents/SenthilKumar/Programs/ParquetwithDict")
  }

}
