package sparkperformance.runlocal

import sparkperformance.builder.SparkContextBuilder

object RunLocalApplication {

  def main(args: Array[String]): Unit = {

    val localRun = SparkContextBuilder.isLocalSpark
    val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Happy Local Spark")

    val numbers = sparkSession.sparkContext.parallelize(Range.apply(1, 1000))

    val total = numbers.sum()

    println(s"Total Value ${total}")


  }

}
