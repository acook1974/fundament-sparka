package module4.udfs

import org.apache.spark.sql.api.java.UDF4;

class CapitalizationAndContributionUDF extends UDF4[Long, Int, Int, Int, Double] {
  override def call(startCapital: Long, yearsAmount: Int, interestRate: Int, contribution: Int): Double = {
    if (startCapital.isNaN || yearsAmount.isNaN || interestRate.isNaN || contribution.isNaN) {
      Double.NaN
    } else if (startCapital < 0 || yearsAmount < 0 || interestRate < 0 || contribution < 0) {
      Double.NaN
    } else {
      val interestRatePerYear = interestRate / 100.0
      val growthFactor = Math.pow(1 + interestRatePerYear, yearsAmount)
      val finalAmount = startCapital * growthFactor + contribution * (growthFactor - 1) / interestRatePerYear
      "%2.2f".format(finalAmount).toDouble
    }
  }
}