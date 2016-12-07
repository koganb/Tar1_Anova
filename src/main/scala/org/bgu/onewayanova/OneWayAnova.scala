package org.bgu.onewayanova

/** Computes one way anova */
object OneWayAnova {
  val logger = LoggerFactory.getLogger(OneWayAnova.getClass)

  def main(args: Array[String]) {

    logger.debug("starting one way anova")

    val fileName = args.headOption

    if (fileName.isDefined) {
      val conf = new SparkConf().setAppName("One Way Anova").
        set("spark.dynamicAllocation.enabled", "false") //fix https://community.hortonworks.com/questions/37247/initial-job-has-not-accepted-any-resources.html

      val sc = new SparkContext(conf)

      val stats: scala.collection.Map[String, Stats] = sc.
        textFile("/tmp/big.txt"). //read file
        map(_.split(",")). //split row to tuple
        map(a => (a(0), Value(a(1).toDouble, math.pow(a(1).toDouble, 2), 1))).
        flatMap(a => Seq(("All", a._2), (a._1, a._2))). //send for each row 2 keys
        reduceByKey((a, b) => Value(a.value + b.value, a.squareValue + b.squareValue, a.count + b.count)).
        mapValues(a => Stats(mean(a.value, a.count), variance(a.value, a.squareValue, a.count), a.count)). //calculate stats for each group
        collectAsMap()

      logger.debug("Got the group stats {}", stats)

      val groupStats = stats.filterKeys(_ != "All")
      val allStats = stats("All")
      val ssb = groupStats.values.foldLeft(0.0)((b, a) => b + scala.math.pow(a.mean - allStats.mean, 2) * a.count)
      val ssv = groupStats.values.foldLeft(0.0)((b, a) => b + a.variance * a.count)
      val F = (ssb / (groupStats.size - 1)) / (ssv / (allStats.count - groupStats.size))

      println("=============================================================================")
      println(s" K : ${groupStats.size} \n N : ${allStats.count} \n F : $F")
      println("=============================================================================")
      sc.stop()
    }
    else {
      println("Please supply file name argument")
    }

  }

  def variance(sumElements: Double, sumSquares: Double, count: Int) = sumSquares / count - math.pow(mean(sumElements, count), 2)

  def mean(sumElements: Double, count: Int) = sumElements / count

  case class Value(value: Double, squareValue: Double, count: Int)

  case class Stats(mean: Double, variance: Double, count: Int)
}