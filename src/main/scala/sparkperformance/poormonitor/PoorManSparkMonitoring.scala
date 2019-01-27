package sparkperformance.poormonitor

import java.util.concurrent.{Callable, Executors, TimeUnit}

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder

object PoorManSparkMonitoring {


  val log = LoggerFactory.getLogger(this.getClass.getName)
  val monitoringExecutor = Executors.newCachedThreadPool()

  def main(args: Array[String]): Unit = {

    try {
      log.info("Starting spark application ")

      monitoringExecutor.submit(newCallable(checkSparkContext))

      val localRun = SparkContextBuilder.isLocalSpark
      Thread.sleep(1000 * 5) //Fake slow spark context creation
      val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Poor man monitoring")
      val pointsRecordCounter = sparkSession.sparkContext.longAccumulator("recordCounter")

      monitoringExecutor.submit(newCallable(() => monitorRecordsProcessed(pointsRecordCounter)))
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

  private def monitorRecordsProcessed(pointsRecordCounter: LongAccumulator): Unit = {
    val startTime = System.currentTimeMillis()
    val counter = pointsRecordCounter
    log.info("Records monitoring started")
    while (true) {
      val timeInSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)
      val recordsCount = counter.sum
      val tp = recordsCount.toFloat / timeInSec
      log.info(s"Records processed ${recordsCount} in ${timeInSec} sec , throughput ${tp} / sec")
      Thread.sleep(TimeUnit.SECONDS.toMillis(1))
    }
  }

  private def newCallable(f: () => Unit): Callable[Unit] = {
    new Callable[Unit] {
      override def call(): Unit = {
        f.apply()
      }

    }
  }

  private def checkSparkContext(): Unit = {
    val startTime = System.currentTimeMillis()
    log.info("Context creation monitor thread started")
    while (SparkContextBuilder.isContextNotCreated()) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(1))
      val total = System.currentTimeMillis() - startTime
      log.info("Waiting for spark context from {} seconds", TimeUnit.MILLISECONDS.toSeconds(total))
    }

    log.info("Spark context creation took {} seconds", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime))
  }
}
