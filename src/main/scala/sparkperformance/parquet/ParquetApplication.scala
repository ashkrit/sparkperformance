package sparkperformance.parquet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder

object ParquetApplication {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    try {
      log.info("Starting spark application ")

      val localRun = SparkContextBuilder.isLocalSpark

      val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Parquet Application")


      val topXValues = processData(args(0), sparkSession)

      log.info("Value computed {}", topXValues)


      sparkSession.close()
      log.info("Spark application completed")
    }
    finally {

    }
  }

  private def processData(path: String, sparkSession: SparkSession) = {
    val topXValues = sparkSession.sparkContext.textFile(path)
      .map(line => line.split(","))
      .map(row => {
        (row(0), 1)
      })
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, false)

    val fields = StructType(Array(
      StructField("id", StringType),
      StructField("count", StringType)
    ))


    topXValues.take(10)
  }

}
