import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkShuffle {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkShuffle")
      .master("local[*]")
      .getOrCreate()
    val tb1 = args(0)
    val tb2 = args(1)
    var range_val = 1000
    if( args(2).toInt > 0 ) {
      range_val = args(2).toInt
    }
    spark.range(range_val)
      .select(col("id"), col("id").as("k"))
      .write
      .partitionBy("k")
      .format("parquet")
      .mode("overwrite")
      .saveAsTable(tb1)
    spark.sql("select * from " + tb1).show()
    spark.range(range_val)
      .select(col("id"), col("id").as("k"))
      .write
      .partitionBy("k")
      .format("parquet")
      .mode("overwrite")
      .saveAsTable(tb2)
    spark.sql("select * from " + tb2).show()
    spark.sql("set spark.optimizer.dynamicPartitionPruning.fallbackFilterRation=2")
    spark.sql("set spark.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false")
    spark.sql("SELECT " +tb1 +".id, " + tb2 +".k FROM " + tb1 + " JOIN " + tb2 + " ON struct("+tb1+".k) = struct("+tb2+".k) AND "+tb2+".id <2").show()
  }

}



