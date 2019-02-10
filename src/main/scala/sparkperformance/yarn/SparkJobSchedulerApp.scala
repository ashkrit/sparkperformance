package sparkperformance.yarn

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

object SparkJobSchedulerApp {

  val WaitTimeThreshHoldInMinute = 2

  def main(args: Array[String]): Unit = {

    val client: YarnClient = new MockYarnClient

    val user = args(0)
    val status = args(1)

    val metrics = client.clusterMetrics()
    val clusterQueueInfo = client.clusterQueue()
    val queueWithCapacity = client.freeQueues(metrics, clusterQueueInfo)

    val selectedYarnApps = client.appsBy(user, status)

    if (hasApps(selectedYarnApps)) {

      val now = System.currentTimeMillis()

      val appsByMaxWaitingTime = selectedYarnApps.apps.app.asScala.map(app =>
        (app, TimeUnit.MILLISECONDS.toMinutes(now - app.startedTime))
      ).filter(v => v._2 > WaitTimeThreshHoldInMinute)
        .sortBy(v => v._2)(Ordering[Long].reverse)


      appsByMaxWaitingTime.foreach { case (app, waitTime) =>
        println(s" Application ${app.id} is waiting for ${waitTime} minutes")
        client.moveApp(app, queueWithCapacity)
      }
    }

  }

  private def hasApps(selectedYarnApps: YarnApps): Boolean = {
    selectedYarnApps.apps != null && selectedYarnApps.apps.app != null
  }
}
