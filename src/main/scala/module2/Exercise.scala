package module2

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD

object Exercise {
  
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleData: Seq[(String, String, String, Int)] = Seq(
      ("John", "Doe", "M", 25),
      ("Jane", "Smith", "F", 30),
      ("Jim", "Beam", "M", 35),
      ("Alice", "Johnson", "F", 28),
      ("Bob", "Brown", "M", 32),
      ("Charlie", "Davis", "M", 29),
      ("Diana", "Evans", "F", 27),
      ("Ethan", "Foster", "M", 31),
      ("Fiona", "Garcia", "F", 26),
      ("George", "Harris", "M", 33),
      ("Helen", "Wilson", "F", 24),
    )
    
    val peopleRDD: RDD[(String, String, String, Int)] = spark.sparkContext.parallelize(peopleData)
    peopleRDD.take(5).foreach(println)

    val sumAgeMen: RDD[Int] = peopleRDD.filter(r => r._3 == "M").map(r => r._4)
    val sumAgeWomen: RDD[Int] = peopleRDD.filter(r => r._3 == "F").map(r => r._4)
    val agesRDD = peopleRDD.map{case(_, _, _, age) => age}

    println("--------------------------------")
    println(s"Suma wieku mężczyzn: ${sumAgeMen.sum()}")
    println(s"Suma wieku kobiet: ${sumAgeWomen.sum()}")
    println(s"Minimalny wiek: ${agesRDD.min()}, maksymalny wiek: ${agesRDD.max()}")
    println("--------------------------------")

  }
}
