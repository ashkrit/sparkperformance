package sparkperformance.builder

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkException}

object SparkContextBuilder {

  val contextCreate = new AtomicBoolean(false)

  def isRunLocal(args: Array[String]): Boolean = {
    if (args.length > 0) {
      args(0).equalsIgnoreCase("local")
    }
    else {
      false
    }
  }

  def isContextNotCreated(): Boolean = {
    !contextCreate.get()
  }

  def isLocalSpark(): Boolean = {
    val runLocalValue = System.getProperties
      .getProperty("spark.local", "false")
      .toBoolean
    runLocalValue
  }


  def newSparkSession(runLocal: Boolean, name: String): SparkSession = {

    val sparkConf = new SparkConf().setAppName(name)
    if (runLocal) {
      sparkConf.setMaster("local[*]")
    }
    try {
      val sparkSession = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()

      contextCreate.set(true)
      sparkSession
    }
    catch {
      case oops: SparkException => {
        if (oops.getMessage.toLowerCase.indexOf("master url") > -1) {
          println("Set local mode using -Dspark.local=true variable")
        }
        println()
        throw oops
      }
    }
  }

}
