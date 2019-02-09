package sparkperformance.yarn

object SparkJobSchedulerApp {

  def main(args: Array[String]): Unit = {
    val client: YarnClient = new MockYarnClient

    val metrics = client.clusterMetrics()
    val clusterQueueInfo = client.clusterQueue()
    val queueWithCapacity = client.freeQueues(metrics, clusterQueueInfo)

    println(queueWithCapacity)
    val acceptedApps = client.appsBy("someuser", "accepted")

    client.moveApp(acceptedApps(0), queueWithCapacity) //Move to available queue

  }

}
