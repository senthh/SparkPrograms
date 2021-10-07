import org.apache.spark.sql.SparkSession

object Extraslash_Issue_803007 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Extraslash_Issue_803007")
      .master("local[*]")
      .getOrCreate()
    val df = spark.read
      .option("header",false)
      .option("quote","")
      .option("escape", "")
      .csv("/Users/senthilkumar/Downloads/Cases/803007/Sample_File.csv")

    df.show(false)


    df.write
      .option("quote","")
      .option("escape", "")
      .format("csv")
      .save("/Users/senthilkumar/Downloads/Cases/803007/Sample_File_new.txt")
  }

}
