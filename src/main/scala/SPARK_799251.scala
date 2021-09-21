import org.apache.spark.sql.SparkSession
import org.json4s.scalap.Main

object SPARK_799251 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("minPartitionNumber")
      .master("local[*]")
      .getOrCreate()

    val bin_rdd = spark.sparkContext.binaryFiles("/Users/senthilkumar/Downloads/DataSets/Images/",2)
    println(bin_rdd.getNumPartitions)
  }
}
