package sparkperformance.dag

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DagExampleApplication {

  val log_ = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val localRun = isRunLocal(args)

    val session = newSparkSession(localRun, "DAG Example Application")

    val totalNumberOfElement = 100000 * 10 * 10
    val numbers = (1 to totalNumberOfElement)
      .map(_ => util.Random.nextInt(totalNumberOfElement))

    val randomNumbers = session.sparkContext.parallelize(numbers)

    val topXNumbers = randomNumbers
      .filter(_ > 1000) //Stage 1
      .map(value => (value, 1)) // Stage 1

      .reduceByKey((x, y) => x + y) //Stage 2
      //.map(value => (value._1, value._2.sum)) //Stage 2

      .sortBy(_._2, false) //Stage 3
      .count() // Stage 3

    log_.info("Count of values {}", topXNumbers)
  }

  private def isRunLocal(args: Array[String]): Boolean = {
    if (args.length > 0) {
      args(0).equalsIgnoreCase("local")
    }
    else {
      false
    }
  }

  private def newSparkSession(runLocal: Boolean, name: String): SparkSession = {

    val sparkConf = new SparkConf().setAppName(name)
    if (runLocal) {
      sparkConf.setMaster("local[*]")
    }
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    sparkSession
  }
}
