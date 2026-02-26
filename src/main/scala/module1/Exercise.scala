package module1

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD

object Exercise {
  
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val people: Seq[(String, String, String, Int, String, Int)] = Seq(
        ("Artur", "Kowalski", "65123012345", 62, "M", 12300),
        ("Anna", "Nowak", "65123012346", 58, "F", 10200),
        ("Piotr", "Janicki", "65123012347", 45, "M", 15000),
        ("Barbara", "Bukowska", "65123012348", 38, "F", 12000),
        ("Tomasz", "Pasierbicki", "65123012349", 25, "M", 18000),
        ("Ewa", "Animowski", "65123012350", 22, "F", 11000),
        ("Jan", "Kutek", "65123012351", 18, "M", 13000),
        ("Maria", "Misiek", "65123012352", 15, "F", 14000),
    )

    val peopleDF: DataFrame = people.toDF("name", "surname", "pesel", "age", "gender", "salary")
    peopleDF.show()

    val ile = peopleDF.count()
    peopleDF.select("surname", "gender", "salary").show(ile.toInt)
    
  }
}
