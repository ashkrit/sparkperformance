package sparkperformance.ml.gender

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import sparkperformance.builder.SparkContextBuilder

import scala.collection.mutable.ArrayBuffer

/*


   This model is trained on - https://github.com/ankane/age/tree/master/names

  Very Good data based on country - https://ideas.repec.org/c/wip/eccode/10.html
   ftp://ftp.heise.de/pub/ct/listings/0717-182.zip

 */

object GenderClassificationModel {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val localRun = SparkContextBuilder.isLocalSpark
    val sparkSession = SparkContextBuilder.newSparkSession(localRun, "Gender Class Application")

    val location = args(0)

    log.info(s"Reading from ${location}")
    val context = sparkSession.sparkContext
    val labelData = context.textFile(location).map(_.split(","))
      .filter(r => r(0).length >= 3)
      .map(GenderFeatureExtractor.buildFeatures(_))
    val df = toDataFrame(sparkSession, labelData)

    val pipeline: Pipeline = configureTrainPipeline(df)

    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
    val model = pipeline.fit(trainingData)
    model.save("gender_v3.model")


    val predictions = model.transform(testData)
    predictions.select("predictedLabel", "gender", "name", "features").show(5)

  }

  private def configureTrainPipeline(df: DataFrame) = {
    val labelIndexer = valueToIndex(df)

    val cols = Array("last", "last2", "last3", "first", "first2", "first3")

    val featureIndexer = cols.map(col => new StringIndexer().setInputCol(col).setOutputCol(s"${col}_index").fit(df))

    val hotEncoders = cols.map(col => new OneHotEncoder().setInputCol(s"${col}_index").setOutputCol(s"${col}_vector"))

    val assembler = new VectorAssembler()
      .setInputCols(cols.map(col => s"${col}_vector"))
      .setOutputCol("features")


    val dt = newClassifier()
    val labelConverter = indexToValue(labelIndexer)

    val stages = new ArrayBuffer[PipelineStage]()
    stages.append(labelIndexer)

    stages.appendAll(featureIndexer)
    stages.appendAll(hotEncoders)
    stages.append(assembler)

    stages.append(dt)
    stages.append(labelConverter)

    val pipeline = new Pipeline().setStages(stages.toArray)
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
    new DecisionTreeClassifier().setLabelCol("gender_index").setFeaturesCol("features")
  }

  private def valueToIndex(df: DataFrame) = {
    new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("gender_index")
      .setHandleInvalid("keep")
      .fit(df)
  }

}
