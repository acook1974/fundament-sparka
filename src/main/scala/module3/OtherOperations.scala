package module3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.catalyst.expressions.Sha2

object OtherOperations {
  
  def pizzaAnalysis(spark: SparkSession): Unit = {
    import spark.implicits._

    val pizzaDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("data/pizza_data.csv")

    pizzaDF.show()
    pizzaDF.printSchema()
    println(s"Number of pizzas: ${pizzaDF.count()}")

    val pizzaCleanDF: Dataset[Row] = pizzaDF.withColumn("Price", regexp_replace(col("Price"), "\\$", "").cast(DoubleType))
    pizzaCleanDF.show()
    pizzaCleanDF.printSchema()
    println(s"Number of pizzas: ${pizzaCleanDF.count()}")

    val companiesWithAvgPricesDF: Dataset[Row] = pizzaCleanDF.groupBy("Company")
      .agg(avg(col("Price")).as("avgPrice"))
    companiesWithAvgPricesDF.show()

    val companiesWithSumPricesDF: Dataset[Row] = pizzaCleanDF.groupBy("Company")
      .agg(sum(col("Price")).as("sumPrice"))
    companiesWithSumPricesDF.show()

    val companiesWithPizzaCountDF: Dataset[Row] = pizzaCleanDF.select("Company", "Pizza Name")
      .distinct()
      .groupBy("Company")
      .agg(count(col("Pizza Name")).as("countPizza"))
      .orderBy(col("countPizza").desc)
    companiesWithPizzaCountDF.show()

    println("--------------------------------")
  }

  def netflixAnalysis2(spark: SparkSession): Unit = {
    import spark.implicits._

    val netflixDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("data/netflix_titles.csv")

    netflixDF.show(truncate = false)
    netflixDF.printSchema()
    println(s"Number of netflix data: ${netflixDF.count()}")
    
    val netflixWithoutNullsDF: Dataset[Row] = netflixDF.na.fill("N/A")
    netflixWithoutNullsDF.show(truncate = false)
    netflixWithoutNullsDF.printSchema()
    println(s"Number of netflix data: ${netflixWithoutNullsDF.count()}")

    val explodedListendInStatsDF: Dataset[Row] = netflixDF.withColumn("listed_in", split(col("listed_in"), ","))
      .select(col("show_id"), explode(col("listed_in")).as("singleListedIn"))
      .withColumn("singleListedIn", trim(col("singleListedIn")))
    explodedListendInStatsDF.show(truncate = false)

    val listedInStatsDF: Dataset[Row] = explodedListendInStatsDF.groupBy("singleListedIn")
      .count()
      .orderBy(col("count").desc)
    listedInStatsDF.show(truncate = false)
    listedInStatsDF.printSchema()

    val hashedDirectorDF: Dataset[Row] = netflixWithoutNullsDF.withColumn("directorHash", sha2(col("director"), 256))
    hashedDirectorDF.show(truncate = false)
    hashedDirectorDF.printSchema()

    hashedDirectorDF.write
      .parquet("data/netflix_hashed_directors.parquet")
  }
}
