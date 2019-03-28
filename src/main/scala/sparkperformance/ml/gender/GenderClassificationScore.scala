package sparkperformance.ml.gender

import org.apache.spark.ml.PipelineModel
import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder

object GenderClassificationScore {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val localRun = SparkContextBuilder.isLocalSpark
    val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Gender Class Application")

    val location = args(0)

    log.info(s"Reading from ${location}")
    val labelData = sparkSession.sparkContext.textFile(location)
      .map(line => line.split(","))
      .map { row => GenderFeatureExtractor.buildFeatures(row) }

    val df = sparkSession.createDataFrame(labelData)
    df.printSchema()

    val model = PipelineModel.load("gender_v2.model")

    val predictions = model.transform(df)

    predictions.select("predictedLabel", "gender", "name", "features").show(5)

  }


}
