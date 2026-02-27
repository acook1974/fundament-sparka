package module3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions._

object Json {
  
  def example1(spark: SparkSession): Unit = {
    import spark.implicits._

    val dataSchema = StructType(
      List(
        StructField("name", DataTypes.StringType, false),
        StructField("time", DataTypes.TimestampType, false)
      )
    )

    val jsonDF = Seq(("{\"name\": \"marek\",\"time\": 1469501675}"), ("{\"name\": \"kasia\",\"time\": 1469501623}")).toDF("value")
    jsonDF.show(truncate = false)
    jsonDF.printSchema()

    jsonDF.withColumn("jsonData", from_json(col("value"), dataSchema)).show(truncate = false)
    jsonDF.withColumn("jsonData", from_json(col("value"), dataSchema)).printSchema()

    val fromJsonDF = jsonDF.withColumn("jsonData", from_json(col("value"), dataSchema)).select("jsonData.*")
    fromJsonDF.show(truncate = false)
    fromJsonDF.printSchema()
  }

}
