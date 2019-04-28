package sparkperformance.ml.gender

import java.util.Scanner

import org.apache.spark.ml.PipelineModel
import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder

object GenderClassificationScore {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val localRun = SparkContextBuilder.isLocalSpark
    val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Gender Class Prediction Application")

    val location = args(0)

    log.info(s"Reading from ${location}")

    val model = PipelineModel.load("gender_v3.model")

    log.info(s"${model.stages(14)}")
    val scanner = new Scanner(System.in)

    val context = sparkSession.sparkContext

    while (true) {
      try {
        val line = scanner.nextLine()
        log.info(s"Check ${line}")

        val rdd = context.parallelize(List(line))
        val labelData = rdd.map(GenderFeatureExtractor.toFeatures(_))
        val df = sparkSession.createDataFrame(labelData)
        val predictions = model.transform(df)

        predictions.select("predictedLabel", "gender", "name", "features").show(5)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }


  }


}
