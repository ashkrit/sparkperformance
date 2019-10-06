package sparkperformance.risk

import java.sql.Date

case class Trade(symbol: String, tradeDate: Date, price: Double)

case class PriceVector(prices: Array[Double], var index: Int = 0) {
  def add(price: Double): Unit = {
    prices(index) = price
    index = index + 1
  }

  def size(): Int = {
    index
  }

  def capacity(): Int = {
    prices.size
  }

  def merge(vector: PriceVector): PriceVector = {
    for (index <- 0 until vector.size()) {
      this.add(vector.prices(index))
    }
    this
  }
}