package module2

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object BasicOperation {
  
  def example1(spark: SparkSession): Unit = {
    import spark.implicits._

    val nameList: Seq[String] = Seq("John", "Jane", "Jim", "Jill", "Jack", "Jill", "Adam", "Alex", "Steve")
    val nameRDD: RDD[String] = spark.sparkContext.parallelize(nameList)

    val namesBig: RDD[String] = nameRDD.map(name => name.toUpperCase())
    
    namesBig
      .take(5)
      .foreach(println)

    val namesFiltered: RDD[String] = nameRDD.filter(name => name.length < 5)
    namesFiltered.take(5).foreach(println)

  }
  
  def example2(spark: SparkSession): Unit = {
    import spark.implicits._

    val numbersList: Seq[Int] = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbersRDD: RDD[Int] = spark.sparkContext.parallelize(numbersList)

    val numbersSum: Double = numbersRDD.sum()
    println(s"Sum of numbers: $numbersSum")

    val numbersReduce: Int = numbersRDD.reduce((a, b) => a * b)
    println(s"Reduce of numbers: $numbersReduce")
    
  }

}