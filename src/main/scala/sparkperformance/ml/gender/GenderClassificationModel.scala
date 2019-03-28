package sparkperformance.ml.gender

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder

object GenderClassificationModel {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val localRun = SparkContextBuilder.isLocalSpark
    val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Gender Class Application")

    val location = args(0)

    log.info(s"Reading from ${location}")
    val context = sparkSession.sparkContext
    val labelData = context.textFile(location).map(_.split(",")).map(GenderFeatureExtractor.buildFeatures(_))
    val df = toDataFrame(sparkSession, labelData)

    val pipeline: Pipeline = configureTrainPipeline(df)

    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
    val model = pipeline.fit(trainingData)
    model.save("gender_v2.model")


    val predictions = model.transform(testData)
    predictions.select("predictedLabel", "gender", "name", "features").show(5)

  }

  private def configureTrainPipeline(df: DataFrame) = {
    val labelIndexer = valueToIndex(df)
    val dt = newClassifier
    val labelConverter = indexToValue(labelIndexer)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, dt, labelConverter))
    pipeline
  }

  private def toDataFrame(sparkSession: SparkSession, labelData: RDD[GenderFeatureExtractor.NameFeatures]) = {
    val df = sparkSession.createDataFrame(labelData)
    df.printSchema()
    df
  }

  private def indexToValue(labelIndexer: StringIndexerModel) = {
    new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
  }

  private def newClassifier() = {
    new DecisionTreeClassifier().setLabelCol("indexedgender").setFeaturesCol("features")
  }

  private def valueToIndex(df: DataFrame) = {
    new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("indexedgender")
      .fit(df)
  }


}
