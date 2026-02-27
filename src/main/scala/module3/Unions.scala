package module3

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession

object Unions {

  def example1(spark: SparkSession): Unit = {
    import spark.implicits._

    val pizza1DF: Dataset[Row] = spark.read.option("header", "true").csv("data/pizza_data_half.csv")
    val pizza2DF: Dataset[Row] = spark.read.option("header", "true").csv("data/pizza_data_half2.csv")

    // val pizzaUnionDF: Dataset[Row] = pizza1DF.union(pizza2DF)
    val pizzaUnionDF: Dataset[Row] = pizza1DF.unionByName(pizza2DF)
    
    val pizza1DFCount: Long = pizza1DF.count()
    val pizza2DFCount: Long = pizza2DF.count()
    val pizzaUnionCount: Long = pizzaUnionDF.count()

    println(s"Number of pizzas in pizza1DF: ${pizza1DFCount}")
    println(s"Number of pizzas in pizza2DF: ${pizza2DFCount}")
    println(s"Number of pizzas in pizzaUnionDF: ${pizzaUnionCount}")

  }
  
}
