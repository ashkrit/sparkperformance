package sparkperformance.partition

import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder

object MapPartitionApplicationFilter {

  val log_ = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val localRun = SparkContextBuilder.isLocalSpark
    val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Map partition Wrong filter")

    val count = sparkSession.sparkContext.textFile(args(0))
      .map(line => line.split(","))
      .mapPartitions(rows => {
        rows.flatMap(row => Some(row))
      }).count()


    log_.info("Count {}", count)

  }
}
