package module4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import module4.udfs.InitialsUDF
import module4.udfs.InterestCapitalizationUDF
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions.{callUDF, col, lit, when}

object UdfsOperations {
  
  def initials(spark: SparkSession): Unit = {
    import spark.implicits._

    var initialsUDF: InitialsUDF = new InitialsUDF()
    spark.udf.register("initialsUDF", initialsUDF, DataTypes.StringType)

    val people: Seq[(String, String, String, Int)] = Seq(("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
      ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
      ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
      ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
      ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
      ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82))

    val peopleDF: Dataset[Row] = people.toDF("id", "firstName", "lastName", "age")
    
    val peopleWithInitialsDF: Dataset[Row] = peopleDF.withColumn("initials", callUDF("initialsUDF", col("firstName"), col("lastName")))
    peopleWithInitialsDF.show()

  }

  def capitalizationMoney(spark: SparkSession): Unit = {
    import spark.implicits._

    val interestCapitalizationUDF: InterestCapitalizationUDF = new InterestCapitalizationUDF()
    spark.udf.register("interestCapitalizationUDF", interestCapitalizationUDF, DataTypes.DoubleType)

    val peopleDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("data/money_saving.csv")

    val peopleWithMoneyDF: Dataset[Row] = peopleDF.withColumn("money", col("money").cast(DataTypes.LongType))
      .withColumn("interest", col("interest").cast(DataTypes.IntegerType))
      .withColumn("10years", callUDF("interestCapitalizationUDF", col("money"), lit(10), col("interest")))
      .withColumn("20years", callUDF("interestCapitalizationUDF", col("money"), lit(20), col("interest")))
      .withColumn("40years", callUDF("interestCapitalizationUDF", col("money"), lit(40), col("interest")))
      .withColumn("60years", callUDF("interestCapitalizationUDF", col("money"), lit(60), col("interest")))

    peopleWithMoneyDF.show()
  }

  def checkingIsAdult(spark: SparkSession): Unit ={
    import spark.implicits._

    val people: Seq[(String, String, String, Int)] = Seq(("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
      ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
      ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
      ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
      ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
      ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82))

    val peopleDF: Dataset[Row] = people.toDF("id", "firstName", "lastName", "age")

    val peopleWithAdultsDF: Dataset[Row] = peopleDF.transform(isAdult)

    peopleWithAdultsDF.show()

  }

  def isAdult(df: Dataset[Row]): Dataset[Row] ={
    df.withColumn("isAdult", when(col("age").geq(18), "T").otherwise("F"))
  }
}
