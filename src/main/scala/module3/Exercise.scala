package module3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object Exercise {
  
  def exercise1(spark: SparkSession): Unit = {
    import spark.implicits._

    val people: Seq[(String, String, String, Int)] = Seq(("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
      ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
      ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
      ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
      ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
      ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82))

    val jobsDF: Dataset[Row] = Seq(("programmer", 0), ("teacher", 18), ("senator", 30), ("president", 35)).toDF("job", "ageLimit")

    val peopleDF: Dataset[Row] = people.toDF("id", "firstName", "lastName", "age")

    val peopleWithLengthNameDF: Dataset[Row] = peopleDF.withColumn("nameLength", length(concat(col("firstName"), col("lastName"))))
    peopleWithLengthNameDF.show()

    val peopleWithJobsDF: Dataset[Row] = peopleWithLengthNameDF.join(jobsDF, peopleWithLengthNameDF("nameLength").leq(jobsDF("ageLimit")), "left")
    peopleWithJobsDF.show()
  }

}
