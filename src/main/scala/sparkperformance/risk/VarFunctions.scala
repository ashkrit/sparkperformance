package sparkperformance.risk

import java.util

object VarFunctions {

  def confidence(vector: Array[Double], levels: Array[Double]): Array[Double] = {

    val size = vector.size
    val newCopy = sortPrice(vector)

    val levelValues = new Array[Double](levels.size)
    for (levelIndex <- 0 until levels.size) {
      val index = Math.round(size * levels(levelIndex)).toInt - 1
      levelValues(levelIndex) = newCopy(index)
    }

    levelValues
  }


  def tail(vector: Array[Double], tail: Int): Array[Double] = {

    val size = vector.size
    val newCopy = sortPrice(vector)
    val startPos = size - tail - 1

    val levelValues = new Array[Double](tail)
    for (levelIndex <- 0 until tail) {
      levelValues(levelIndex) = newCopy(startPos + levelIndex)
    }
    levelValues
  }

  def head(vector: Array[Double], head: Int): Array[Double] = {

    val newCopy = sortPrice(vector)

    val levelValues = new Array[Double](head)
    for (levelIndex <- 0 until head) {
      levelValues(levelIndex) = newCopy(levelIndex)
    }
    levelValues
  }


  private def sortPrice(vector: Array[Double]): Array[Double] = {
    val newCopy = vector.clone()
    util.Arrays.sort(newCopy)
    newCopy
  }
}
