package module1

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object People {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val people: Seq[(String, String, Int)] = Seq(
      ("John", "Doe", 25),
      ("Jane", "Smith", 30),
      ("Jim", "Beam", 35),
      ("Alice", "Johnson", 28),
      ("Bob", "Brown", 32),
      ("Charlie", "Davis", 29),
      ("Diana", "Evans", 27),
      ("Ethan", "Foster", 31),
      ("Fiona", "Garcia", 26),
      ("George", "Harris", 33),
    )

    val peopleDF: Dataset[Row] = people.toDF("firstName", "lastName", "age")

    peopleDF.show()
    peopleDF.printSchema()
  }
}
