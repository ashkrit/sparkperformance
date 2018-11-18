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

case class GroupByValue(var count: Int, val days: mutable.Set[Int])

object ParkingTicketApplicationValueObject {

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
    saveData(aggValue, now)


  }

  private def saveData(aggValue: RDD[(String, GroupByValue)], now: String) = {
    aggValue.mapPartitions(rows => {

      val buffer = new StringBuffer()
      rows.map {
        case (key, value) =>
          buffer.setLength(0)
          buffer
            .append(key).append("\t")
            .append(value.count).append("\t")
            .append(value.days.mkString(","))

          buffer.toString
      }
    })
      .coalesce(100)
      .saveAsTextFile(s"/data/output/${now}", classOf[GzipCodec])
  }

  private def mergeValues(value1: GroupByValue, value2: GroupByValue): GroupByValue = {
    if (value2.days.size > value1.days.size) {
      value2.count = value1.count + value2.count
      value1.days.foreach(d => value2.days.add(d))
      value2
    }
    else {
      value1.count = value1.count + value2.count
      value2.days.foreach(d => value1.days.add(d))
      value1
    }

  }

  private def toPlateLevelData(fieldOffset: Map[String, Int], aggFieldsOffset: Map[String, Int], row: Array[String]) = {

    var result: (String, GroupByValue) = null
    try {
      val issueDate = LocalDate.parse(row(aggFieldsOffset.get("issue date").get), ISSUE_DATE_FORMAT)
      val issueDateValues = mutable.Set[Int]()
      issueDateValues.add(issueDate.toEpochDay.toInt)

      val key = fieldOffset.map(fieldInfo => row(fieldInfo._2)).mkString(",")
      result = (key, new GroupByValue(1, issueDateValues))
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
