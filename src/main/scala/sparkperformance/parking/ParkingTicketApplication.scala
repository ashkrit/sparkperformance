package sparkperformance.parking

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder

import scala.collection.mutable

/*
    Based on data from - https://www.kaggle.com/new-york-city/nyc-parking-tickets
    Download CSV and run it.
 */

object ParkingTicketApplication {

  val log_ = LoggerFactory.getLogger(this.getClass.getName)

  val fields = Array("Plate ID").map(f => f.toLowerCase.trim)
  val aggFields = Array("Issue Date").map(f => f.toLowerCase.trim)

  def main(args: Array[String]): Unit = {

    val localRun = SparkContextBuilder.isRunLocal(args)
    val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Parking Ticket")
    val parameterIndex = inputParameterIndex(localRun)
    val partitionCount = noOfPartition(args, parameterIndex)


    val fileData = sparkSession.sparkContext.textFile(args(parameterIndex))

    val repartitionData = partitionCount match {
      case x if x > -1 => fileData.repartition(partitionCount)
      case _ => fileData
    }

    val headers: List[String] = dataHeaders(fileData)
    log_.info("Headers field {}", headers)

    val fieldOffset = fields.map(field => (field, headers.indexOf(field))).toMap
    log_.info("Field offset {}", fieldOffset)

    val aggFieldsOffset = aggFields.map(field => (field, headers.indexOf(field))).toMap
    log_.info("Agg Field offset {}", aggFieldsOffset)

    val rows = repartitionData.map(line => line.split(","))
    val plateLevelData = rows.map(toPlateLevelData(fieldOffset, aggFieldsOffset, _))
    val aggValue = plateLevelData.reduceByKey((value1, value2) => mergeValues(value1, value2))

    aggValue.take(100).foreach(row => log_.info("Row {}", row))

  }

  private def mergeValues(value1: (Int, mutable.Set[String]), value2: (Int, mutable.Set[String])) = {
    val newCount = value1._1 + value2._1
    val dates = value1._2
    dates.foreach(d => value2._2.add(d))

    (newCount, value2._2)
  }

  private def toPlateLevelData(fieldOffset: Map[String, Int], aggFieldsOffset: Map[String, Int], row: Array[String]) = {
    val issueDate = row(aggFieldsOffset.get("issue date").get)
    val issueDateValues = mutable.Set[String]()
    issueDateValues.add(issueDate)

    (fieldOffset.map(fieldInfo => row(fieldInfo._2)).mkString(","), (1, issueDateValues))
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
