package module4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, DataTypes, LongType}
import module4.udfs.CountWords

object Exercise {
  
  def exercise(spark: SparkSession): Unit = {
    import spark.implicits._

    // 1. Wczytaj do Dataframe’a plik z tytułami Netflix (netflix_titles.csv)
    val netflixDF: Dataset[Row] = spark.read.option("header", "true").csv("data/netflix_titles.csv")
    println(s"--- 1. Wczytaj do Dataframe’a plik z tytułami Netflix (netflix_titles.csv)")
    netflixDF.show()

    // 2. Policz średnią długość opisu filmu licząc w wyrazach.
    val averageDescriptionLengthDF: Dataset[Row] = netflixDF.withColumn("descriptionLength", size(split(trim(col("description")), "\\s+")).cast(IntegerType))
      .agg(avg(col("descriptionLength")).as("averageDescriptionLength"))
    println(s"--- 2. Policz średnią długość opisu filmu licząc w wyrazach.")
    averageDescriptionLengthDF.show()

    // 3. Policz średnią długość opisu filmu licząc w wyrazach wykorzystująć UDF.
    val countWordsUDF: CountWords = new CountWords()
    spark.udf.register("countWordsUDF", countWordsUDF, LongType)

    val averageDescriptionLengthUDF: Dataset[Row] = netflixDF.withColumn("descriptionLength", callUDF("countWordsUDF", col("description")))
      .agg(avg(col("descriptionLength")).as("averageDescriptionLength"))
    println(s"--- 3. Policz średnią długość opisu filmu licząc w wyrazach wykorzystująć UDF.")
    averageDescriptionLengthUDF.show()
  }
}
