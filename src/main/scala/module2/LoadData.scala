package module2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

object LoadData {
  
  def example1(spark: SparkSession): Unit = {
    import spark.implicits._

    val pizzaDF: DataFrame = spark.read.option("header", "true").csv("data/pizza_data.csv")

    pizzaDF.show(truncate = false)
    pizzaDF.printSchema()

  }
  
  def example2(spark: SparkSession): Unit = {
    import spark.implicits._

    val pizzaDF: DataFrame = spark.read.option("header", "true").csv("data/pizza_data.csv")

    val pizzaDFWithFilter: Dataset[Row] = pizzaDF.filter(col("Type").contains("Cheese"))
    val pizzaCount = pizzaDFWithFilter.count()

    pizzaDFWithFilter.show(pizzaCount.toInt, truncate = false)
    pizzaDFWithFilter.printSchema()

    println(s"Number of pizzas: ${pizzaCount}")
  }

}
