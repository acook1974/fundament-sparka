import org.apache.spark.sql.SparkSession

import module1.People
import module1.{Exercise => Exercise1}
import module2.BasicOperation
import module2.LoadData
import module2.{Exercise => Exercise2}

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
    Exercise2.run(spark)
    
    spark.stop()

  }

}