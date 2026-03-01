package module4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, DataTypes, LongType}
import module4.udfs.CountWords
import module4.udfs.CapitalizationAndContributionUDF

object Exercise {
  
  def exercise1(spark: SparkSession): Unit = {
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

  def exercise2(spark: SparkSession): Unit = {
    import spark.implicits._

    val capitalizationAndContributionUDF: CapitalizationAndContributionUDF = new CapitalizationAndContributionUDF()
    spark.udf.register("capitalizationAndContributionUDF", capitalizationAndContributionUDF, DataTypes.DoubleType)

    val peopleDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("data/money_saving.csv")

    val peopleWithMoneyDF: Dataset[Row] = peopleDF.withColumn("money", col("money").cast(DataTypes.LongType))
      .withColumn("interest", col("interest").cast(DataTypes.IntegerType))
      .withColumn("10years", callUDF("capitalizationAndContributionUDF", col("money"), lit(10), col("interest"), lit(1000)))
      .withColumn("20years", callUDF("capitalizationAndContributionUDF", col("money"), lit(20), col("interest"), lit(1000)))
      .withColumn("40years", callUDF("capitalizationAndContributionUDF", col("money"), lit(40), col("interest"), lit(1000)))
      .withColumn("60years", callUDF("capitalizationAndContributionUDF", col("money"), lit(60), col("interest"), lit(1000)))

    peopleWithMoneyDF.show()
  }
}
