import org.apache.spark.sql.SparkSession

object SPARK_22357 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("minPartition analysis")
      .master("local[*]")
      .getOrCreate()

    val binFile = spark.read.option("minPartitions", 2).format("binaryFile").load("/Users/senthilkumar/Desktop/Ghost_Rider.png")
    binFile.repartition('a')

    val num :Int = 'a'
    //Math.
    if(num.isWhole())
      println("Valid Int")
    else
      println("Not a Valid Int")
    println(binFile.rdd.getNumPartitions)
    //println(isNumber)
    //binFile.show()
  }

}
