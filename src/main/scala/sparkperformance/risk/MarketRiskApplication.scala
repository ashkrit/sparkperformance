package sparkperformance.risk

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sparkperformance.builder.SparkContextBuilder

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MarketRiskApplication {

  val TIME_WINDOW = 265

  private[this] val LOG_BASE = 1.1
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
    * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
    */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }


  def main(args: Array[String]): Unit = {

    val x = compressSize(256)
    println(decompressSize(x))

    /*
    val localRun = SparkContextBuilder.isLocalSpark
    val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Market Risk Application")

    val tradeData = loadTradeData
    calculateValueAtRisk(tradeData, sparkSession, (sparkSession) => loadStocksReferenceData(args(0), sparkSession))
    */

  }

  private def calculateValueAtRisk(tradeData: Seq[Trade],
                                   sparkSession: SparkSession,
                                   stocksDataProvider: SparkSession => RDD[(String, Stock)]): Unit = {

    val stocks = stocksDataProvider(sparkSession)

    val confidenceLevels = Array(0.95, 0.97, 0.99)
    val trades = sparkSession.sparkContext.parallelize(tradeData)

    val addValue: (PriceVector, Trade) => PriceVector = (container, trade) => {
      container.add(trade.price)
      container
    }

    trades
      .map(trade => (trade.symbol, trade))
      .aggregateByKey(PriceVector(new Array[Double](TIME_WINDOW)))(addValue, (container1, container2) => {
        container1.merge(container2)
      })
      .map { case (stock, vector) => (stock, VarFunctions.confidence(vector.prices, confidenceLevels)) }
      .join(stocks)
      .foreach {
        case (_, (riskNumbers, stock)) => println(s"Stock ${stock} -> Var ${riskNumbers.mkString(",")}")
      }
  }

  private def loadStocksReferenceData(location: String, sparkSession: SparkSession): RDD[(String, Stock)] = {

    sparkSession.sparkContext
      .textFile(location)
      .map(line => line.split(","))
      .map(row => (row(0), Stock(row(0), row(1), row(2), row(3))))
  }

  private def loadTradeData(): Seq[Trade] = {

    val positionData = new ArrayBuffer[Trade]()

    for (trade <- stocks) {
      var tradeDate = LocalDate.now()
      val rnd = new Random()
      val startPrice = stockStartPrice(trade.symbol)

      for (_ <- 0 until TIME_WINDOW) {
        val javaDate = java.sql.Date.valueOf(tradeDate)
        positionData.append(Trade(trade.symbol, javaDate, startPrice + rnd.nextDouble() * 100))
        tradeDate = tradeDate.minusDays(1)
      }
    }
    positionData
  }

  val stocks = List(
    Stock("MSFT", "Microsoft Corporation", "Technology", "Software - Infrastructure"),
    Stock("GOOGL", "Alphabet Inc", "Technology", "Internet Content & Information"),
    Stock("AAPL", "Apple Inc", "Technology", "Consumer Electronics"),
    Stock("TWTR", "Twitter Inc", "Technology", "Internet Content & Information"),
    Stock("FB", "Facebook Inc", "Technology", "Internet Content & Information"))

  val stockStartPrice: Map[String, Double] = Map(
    "FB" -> 177.75d,
    "TWTR" -> 41,
    "AAPL" -> 202.65,
    "GOOGL" -> 1153.58,
    "MSFT" -> 133.39
  )

}
