import org.apache.spark.sql.functions.{mean, stddev}

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
  }
}

object Movie extends App {
  import org.apache.spark.sql.SparkSession
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Movie")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def analyze(path: String): Unit = {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
    df.select(mean("rating").as("average_rating"), stddev("rating").as("standard_deviation")).show()
  }

  analyze("src/main/scala/ratings_small.csv")

  analyze("src/main/scala/ratings.csv")

}