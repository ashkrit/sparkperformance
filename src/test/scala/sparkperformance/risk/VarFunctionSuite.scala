package sparkperformance.risk

import java.util

import org.scalatest._

import scala.util.Random

class VarFunctionSuite extends FlatSpec with Matchers {

  val NINTY_FIVE = 0.95d
  val NINTY_SEVEN = 0.97d
  val NINTY_NINE = 0.99d
  val indexOffSet = Map(0.95d -> 251, NINTY_SEVEN -> 256, NINTY_NINE -> 261)

  "A var functions" should "calculate var at 95%" in {

    val priceVector = (0 until 265)
      .map(_ => new Random().nextDouble())
      .toArray

    val varValues = VarFunctions.confidence(priceVector, Array(NINTY_FIVE, NINTY_SEVEN, NINTY_NINE))

    util.Arrays.sort(priceVector)

    varValues shouldEqual Array(
      priceVector(indexOffSet(NINTY_FIVE)),
      priceVector(indexOffSet(NINTY_SEVEN)),
      priceVector(indexOffSet(NINTY_NINE))
    )
  }


}
