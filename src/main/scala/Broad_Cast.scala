import org.apache.spark.sql.SparkSession

object Broad_Cast {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("BroadCast").master("local[*]").getOrCreate()
    val states = Map(("NY","New York"),("IL","Illinois"),("CA","California"))
    val bStates = spark.sparkContext.broadcast(states)
    val data = Seq(
                    ("John","Software Engineer","CA"),
                    ("Jerry","Project Manager","IL"),
                    ("Emily","Developer","NY")
                  )
    val columns = Seq("name","role","state")

    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)

    val df2 = df.map(row=>{
      val state = row.getString(2)
      val stateName = bStates.value.get(state).get
      (row.getString(0), row.getString(1), stateName)
    }).toDF(columns:_*)

    df2.show(true)


  }
}
