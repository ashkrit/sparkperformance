package sparkperformance.builder

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkContextBuilder {

  def isRunLocal(args: Array[String]): Boolean = {
    if (args.length > 0) {
      args(0).equalsIgnoreCase("local")
    }
    else {
      false
    }
  }


  def newSparkSession(runLocal: Boolean, name: String): SparkSession = {

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
