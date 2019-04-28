package sparkperformance.parking

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder

import scala.collection.mutable

/*
    Based on data from - https://www.kaggle.com/new-york-city/nyc-parking-tickets
    Download CSV and run it.
 */

object ParkingTicketApplicationOptimized {

  val log_ = LoggerFactory.getLogger(this.getClass.getName)

  val fields = Array("Plate ID").map(f => f.toLowerCase.trim)
  val aggFields = Array("Issue Date").map(f => f.toLowerCase.trim)

  val ISSUE_DATE_FORMAT = DateTimeFormatter.ofPattern("MM/dd/yyyy")

  def main(args: Array[String]): Unit = {

    val localRun = SparkContextBuilder.isRunLocal(args)
    val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Parking Ticket")
    val parameterIndex = inputParameterIndex(localRun)
    val partitionCount = noOfPartition(args, parameterIndex)

    val fileData = sparkSession.sparkContext.textFile(args(parameterIndex))



    val repartitionData = partitionCount match {
      case x if x > -1 => fileData.coalesce(partitionCount)
      case _ => fileData
    }

    val headers: List[String] = dataHeaders(fileData)
    log_.info("Headers field {}", headers)

    val fieldOffset = fields.map(field => (field, headers.indexOf(field))).toMap
    log_.info("Field offset {}", fieldOffset)

    val aggFieldsOffset = aggFields.map(field => (field, headers.indexOf(field))).toMap
    log_.info("Agg Field offset {}", aggFieldsOffset)


    val rows = repartitionData.map(line => line.split(","))
    val plateLevelData = rows.mapPartitions(rows => {
      val result = rows
        .map(row => toPlateLevelData(fieldOffset, aggFieldsOffset, row))
        .filter(x => x != null)
      result
    }
    )


    val aggValue = plateLevelData.reduceByKey((value1, value2) => mergeValues(value1, value2))
    val now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmm"))

    aggValue
      .map { case (key, value) => Array(key, value._1, value._2.mkString(",")).mkString("\t") }.coalesce(100)
      .saveAsTextFile(s"/data/output/${now}", classOf[GzipCodec])


  }


  private def mergeValues(value1: (Int, mutable.Set[Int]), value2: (Int, mutable.Set[Int])): (Int, mutable.Set[Int]) = {
    val newCount = value1._1 + value2._1
    val dates = value1._2

    dates.foreach(d => value2._2.add(d))

    (newCount, value2._2)
  }

  private def toPlateLevelData(fieldOffset: Map[String, Int], aggFieldsOffset: Map[String, Int], row: Array[String]) = {

    var result: (String, (Int, mutable.Set[Int])) = null
    try {
      val issueDate = LocalDate.parse(row(aggFieldsOffset.get("issue date").get), ISSUE_DATE_FORMAT)
      val issueDateValues = mutable.Set[Int]()
      issueDateValues.add(issueDate.toEpochDay.toInt)
      result = (fieldOffset.map(fieldInfo => row(fieldInfo._2)).mkString(","), (1, issueDateValues))
    } catch {
      case _: Exception => {
        //Count bad records
      }
    }
    result
  }

  private def dataHeaders(fileData: RDD[String]): List[String] = {
    fileData.take(1)
      .flatMap(line => line.split(","))
      .map(field => field.toLowerCase.trim).toList
  }

  private def inputParameterIndex(localRun: Boolean) = {
    localRun match {
      case true => 1
      case _ => 0
    }

  }

  def noOfPartition(args: Array[String], parameterIndex: Int): Int = {
    if (args.length > parameterIndex + 1) {
      args(parameterIndex + 1).toInt
    }
    else {
      -1
    }

  }

}
