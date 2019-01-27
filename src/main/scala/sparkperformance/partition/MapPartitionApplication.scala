package sparkperformance.partition

import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder

object MapPartitionApplication {

  val log_ = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val localRun = SparkContextBuilder.isLocalSpark
    val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Map partition Magic")

    val totalNumberOfElement = 10
    val numbers = (1 to totalNumberOfElement)
      .map(_ => util.Random.nextInt(totalNumberOfElement))

    val randomNumbers = sparkSession.sparkContext.parallelize(numbers).repartition(2)

    val total = randomNumbers.mapPartitions(data => {
      val by2 = data.map(x => {
        log_.info("Processing {}", x)
        x * 2
      })
      by2
    }).sum()


    log_.info("Total Value {}", total)

  }
}
