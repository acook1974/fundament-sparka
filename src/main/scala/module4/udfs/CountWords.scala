package module4.udfs

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions._

class CountWords extends UDF1[String, Long] {
  
  override def call(description: String): Long = {
    if (description == null) {
      0
    } else {
      description.trim.split("\\s+").length.toLong
    }
  }

}
