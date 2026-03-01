import org.apache.spark.sql.SparkSession

import module1.People
import module1.{Exercise => Exercise1}
import module2.BasicOperation
import module2.LoadData
import module2.{Exercise => Exercise2}
import module3.Unions
import module3.Joining
import module3.OtherOperations
import module3.{Exercise => Exercise3}
import module3.Json
import module3.{People => People3}
import module4.{UdfsOperations => UdfsOperations}
import module4.{Exercise => Exercise4}

object App {  

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
        .appName("fundament-sparka")
        .master("local")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") 

    // module1
    // People.run(spark)
    // Exercise1.run(spark)

    // module2
    // BasicOperation.example1(spark)
    // BasicOperation.example2(spark)
    // LoadData.example1(spark)
    // LoadData.example2(spark)
    // Exercise2.run(spark)
    // Unions.example1(spark)
    // Joining.example1(spark)
    // Joining.example2(spark)
    // OtherOperations.pizzaAnalysis(spark)
    // OtherOperations.netflixAnalysis2(spark)
    // Exercise3.exercise1(spark)
    // Json.example1(spark)
    // People3.run(spark)
    // Exercise3.exercise2(spark)
    // Exercise3.exercise3(spark)
    // UdfsOperations.initials(spark)
    // UdfsOperations.capitalizationMoney(spark)
    // udfsOperations.checkingIsAdult(spark)
    // Exercise4.exercise1(spark)
    Exercise4.exercise2(spark)

    spark.stop()

  }

}