package sparkperformance.ml.gender

object GenderFeatureExtractor {


  case class NameFeatures(gender: String, name: String,
                          last: String, last2: String, last3: String, first: String, first2: String, first3: String)

  def suffix(value: String, len: Int): String = {
    if (value.length >= len) {
      value.substring(value.length - len);
    }
    else {
      " "
    }
  }

  def prefix(value: String, len: Int): String = {
    if (value.length >= len) {
      value.substring(0, len)
    }
    else {
      " "
    }
  }

  def buildFeatures(row: Array[String]): NameFeatures = {
    val name = row(0)
    val nameValue = name.toLowerCase()

    NameFeatures(row(1), name,
      nameValue.last.toString, suffix(nameValue, 2), suffix(nameValue, 3),
      nameValue.head.toString, prefix(nameValue, 2), prefix(nameValue, 3))

  }

  def toFeatures(name: String): NameFeatures = {

    val nameValue = name.toLowerCase()
    NameFeatures("???", name,
      nameValue.last.toString, suffix(nameValue, 2), suffix(nameValue, 3),
      nameValue.head.toString, prefix(nameValue, 2), prefix(nameValue, 3))

  }

}
