package sparkperformance.ml.gender

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors

object GenderFeatureExtractor {

  val vowelChars = Set('a', 'e', 'i', 'o', 'u')
  val consonantsChars = Set('z', 'b', 't', 'g', 'h')


  def hasVowel(lastChar: Char): Double = {
    if (vowelChars.contains(lastChar))
      1.0
    else
      0.0
  }

  def hasConsonant(lastChar: Char): Double = {
    if (consonantsChars.contains(lastChar))
      1.0
    else
      0.0
  }

  def withL(firstChar: Char): Double = {
    if (firstChar == 'l') 1.0
    else 0.0
  }

  def withN(char: Char): Double = {
    if (char == 'n') 1.0
    else 0.0
  }


  def withValues(value: String, matchValues: String): Double = {
    if (value.equals(matchValues)) 1.0
    else 0.0
  }

  case class NameFeatures(gender: String, name: String, features: linalg.Vector)

  def buildFeatures(row: Array[String]): NameFeatures = {
    val name = row(0)
    val nameValue = name.toLowerCase()
    val first = nameValue.head
    val last = name.last
    val suffix2 = name.substring(name.length - 2)

    val features = Array(
      GenderFeatureExtractor.hasVowel(last),
      GenderFeatureExtractor.hasConsonant(last),
      GenderFeatureExtractor.withL(first),
      GenderFeatureExtractor.withN(last),
      GenderFeatureExtractor.withValues(suffix2, "yn"),
      GenderFeatureExtractor.withValues(suffix2, "ch"))
    val vector: linalg.Vector = Vectors.dense(features)
    NameFeatures(row(1), nameValue, vector)
  }
}
