import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object MergeSortCheck {
  def main(args: Array[String]): Unit = {
    val table1Key = "t1_key"
    val table1Value = "t1_value"

    val table2Key = "t2_key"
    val table2Value = "t2_value"
    val proc = Runtime.getRuntime.availableProcessors()
    println("Number of Processors in Mac : " + proc)
    val spark = SparkSession.builder()
      .appName("BroadCast")
      .master(s"local[${Runtime.getRuntime.availableProcessors()}]")
      .getOrCreate()

    val table1schema = StructType(List(
      StructField(table1Key, IntegerType),
      StructField(table1Value, DoubleType)
    ));

    val table2schema = StructType(List(
      StructField(table2Key, IntegerType),
      StructField(table2Value, DoubleType)
    ));

    val table1 = spark.sqlContext.createDataFrame(
      rowRDD = spark.sparkContext.parallelize(Seq(
        Row(1, 2.0)
      )),
      schema = table1schema
    )

    val table2 = spark.sqlContext.createDataFrame(
      rowRDD = spark.sparkContext.parallelize(Seq(
        Row(1, 4.0)
      )),
      schema = table2schema
    )

    println("Number of Partitions: " + table1.rdd.getNumPartitions)
    println("Size of a Partition: " + table1.rdd.partitions.size)
    table1.show()
    println("Size of a Partition: " + table1.repartition(col(table1Key)).rdd.partitions.size)
    println("Number of Partitions: " + table1.repartition(col(table1Key)).rdd.getNumPartitions)

    val t1 = table1.repartition(col(table1Key)).groupBy(table1Key).avg()
    val t2 = table2.repartition(col(table2Key)).groupBy(table2Key).avg()

    val joinedDF = t1 join t2 where t1(table1Key) === t2(table2Key)
    joinedDF.explain()
    joinedDF.show()

  }

}
