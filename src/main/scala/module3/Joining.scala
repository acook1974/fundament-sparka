package module3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object Joining {
  
  def example1(spark: SparkSession): Unit = {
    import spark.implicits._

    val people: Seq[(String, String, String, Int)] = Seq(("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
      ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
      ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
      ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
      ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
      ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82))

    val jobsDF: Dataset[Row] = Seq(("1", "teacher"), ("2", "programmer"), ("3", "teacher"), ("4", "architect"), ("5", "director"),
      ("6", "director"), ("7", "architect"), ("8", "programmer"), ("9", "programmer"), ("10", "unemployed"),
      ("11", "teacher"), ("12", "director"), ("13", "programmer"), ("19", "programmer"), ("20", "teacher")).toDF("id", "job")

    val peopleDF: Dataset[Row] = people.toDF("id", "firstName", "lastName", "age")

    peopleDF.show()
    jobsDF.show()

    val innerJoin: Dataset[Row] = peopleDF.join(jobsDF, "id")
    val innerJoin2: Dataset[Row] = peopleDF.join(jobsDF, Seq("id"), "inner")
    val leftJoin: Dataset[Row] = peopleDF.join(jobsDF, Seq("id"), "left_outer")
    val rightJoin: Dataset[Row] = peopleDF.join(jobsDF, Seq("id"), "right_outer")
    val fullJoin: Dataset[Row] = peopleDF.join(jobsDF, Seq("id"), "full")
    val crossJoin: Dataset[Row] = peopleDF.crossJoin(jobsDF)
    val semiJoin: Dataset[Row] = peopleDF.join(jobsDF, Seq("id"), "left_semi")
    val antiJoin: Dataset[Row] = peopleDF.join(jobsDF, Seq("id"), "left_anti")


    innerJoin.show()
    leftJoin.show()
    rightJoin.show()
    fullJoin.show(40)
    crossJoin.show(100)
    println(crossJoin.count())
    semiJoin.show()
    antiJoin.show()
  }

  def example2(spark: SparkSession): Unit = {
    import spark.implicits._

    val people: Seq[(String, String, String, Int)] = Seq(("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
      ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
      ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
      ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
      ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
      ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82))

    val jobsDF: Dataset[Row] = Seq(("programmer", 0), ("teacher", 18), ("senator", 30), ("president", 35)).toDF("job", "ageLimit")

    val peopleDF: Dataset[Row] = people.toDF("id", "firstName", "lastName", "age")

    val peopleWithJobsDF: Dataset[Row] = peopleDF.join(jobsDF, peopleDF("age").geq(jobsDF("ageLimit")), "left")

    val cols: Seq[Column] = peopleDF.schema
      .fieldNames
      .map(name=> col(name))

    peopleWithJobsDF.show()

    val peopleWithGroupedJobs: Dataset[Row] = peopleWithJobsDF.groupBy("id", "firstName", "lastName", "age")
      .agg(collect_list(col("job")).as("jobs"))
    val peopleWithGroupedJobs2: Dataset[Row] = peopleWithJobsDF.groupBy(cols: _*)
      .agg(collect_list(col("job")).as("jobs"))

    peopleWithGroupedJobs.show(false)
    peopleWithGroupedJobs2.show(false)

  }

}
