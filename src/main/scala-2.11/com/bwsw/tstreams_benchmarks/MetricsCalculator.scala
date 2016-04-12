package com.bwsw.tstreams_benchmarks

import Math.sqrt

/**
 * Benchmark trait for basic stat methods
 */
trait MetricsCalculator {
  /**
   *
   * @param data Data to calc stat
   * @return Variance
   */
  def getVariance(data : List[Long]): Double = {
    val avg: Double = data.sum.toDouble / data.size.toDouble
    var res: Double = 0.0
    data.foreach{ x=>
      res += (x-avg)*(x-avg)
    }
    res /= data.size.toDouble
    res = sqrt(res)
    res
  }

  /**
   *
   * @param data Data to calc stat
   * @param percent Percentile param
   * @return Percentile
   */
  def getPercentile(data : List[Long], percent : Double): Long = {
    assert(percent >= 0.0 && percent <= 1)
    val index = ((data.size.toDouble-1) * percent).toInt
    val sorted = data.sorted
    sorted(index)
  }

  /**
   *
   * @param data Data to calc stat
   */
  def getMedian(data : List[Long]) = {
    val avg: Double = data.sum.toDouble / data.size.toDouble
    avg
  }

  /**
   *
   * @param data Data to calc stat
   * @return Sum
   */
  def getSum(data : List[Long]): Long = data.sum
}
