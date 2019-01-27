package sparkperformance.poormonitor

import java.util.concurrent.Executors

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder


object SparkMonitoringAdvance {

  val log = LoggerFactory.getLogger(this.getClass.getName)
  val monitoringExecutor = Executors.newCachedThreadPool()

  def main(args: Array[String]): Unit = {

    try {
      log.info("Starting spark application ")

      val localRun = SparkContextBuilder.isLocalSpark
      val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Poor man monitoring")
      sparkSession.sparkContext.addSparkListener(new StageAccumulatorListener)
      val pointsRecordCounter = sparkSession.sparkContext.longAccumulator("recordCounter")

      val topXValues = processData(args(0), pointsRecordCounter, sparkSession)

      log.info("Value computed {}", topXValues)
      log.info(s"Records processed ${pointsRecordCounter}")

      sparkSession.close()
      log.info("Spark application completed")
    }
    finally {
      monitoringExecutor.shutdownNow()
    }
  }

  private def processData(path: String, pointsRecordCounter: LongAccumulator, sparkSession: SparkSession) = {
    val topXValues = sparkSession.sparkContext.textFile(path)
      .map(line => line.split(","))
      .map(row => {
        pointsRecordCounter.add(1)
        (row(0), 1)
      })
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, false)
      .take(10)
    topXValues
  }
}
