package org.bgu.onewayanova


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

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

            val stats: Map[String, (String, org.apache.spark.util.StatCounter)] = sc.
                textFile(fileName.get). //read file
                map(_.split(",")).      //split row to tuple
                flatMap(a => Seq(("All", a(1).toDouble), (a(0), a(1).toDouble))). //send for each row 2 keys
                groupByKey(). //group by 'All records' and one per each group
                mapValues(value => org.apache.spark.util.StatCounter(value)). //calculate stats for each group
                collect(). //convert to List
                groupBy(_._1). //group by first element of tuple
                mapValues(_ (0)) //flatten map values from list of one element

            logger.debug("Got the group stats {}", stats)

            val groupStats = stats.filterKeys(_ != "All")
            val allStats = stats("All")._2
            val ssb = groupStats.values.foldLeft(0.0)((b, a) => b + scala.math.pow(a._2.mean - allStats.mean, 2) * a._2.count)
            val ssv = groupStats.values.foldLeft(0.0)((b, a) => b + scala.math.pow(a._2.stdev, 2) * a._2.count)
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
}