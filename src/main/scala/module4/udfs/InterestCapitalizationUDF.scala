package module4.udfs

import org.apache.spark.sql.api.java.UDF3;

class InterestCapitalizationUDF extends UDF3[Long, Int, Int, Double] {
  override def call(startCapital: Long, yearsAmount: Int, interestRate: Int): Double = {
    "%2.2f".format(startCapital * Math.pow(1 + interestRate / 100.0, yearsAmount)).toDouble
  }
}
