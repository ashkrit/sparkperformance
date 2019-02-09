package sparkperformance.yarn

import scala.collection.JavaConverters._

// Based on APi - https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
trait YarnClient {

  def clusterMetrics(): ClusterMetrics

  def clusterQueue(): ClusterScheduler

  def freeQueues(metrics: ClusterMetrics, clusterQueueInfo: ClusterScheduler): List[Queue] = {

    val queues = clusterQueueInfo.scheduler.schedulerInfo.queues
    val cores = metrics.clusterMetrics.totalVirtualCores
    val queueWithCapacity = queues.queue.asScala.map(q => {
      q.capacityCores = ((cores * q.maxCapacity) / 100).toLong
      val usedCores = ((cores * q.usedCapacity) / 100).toLong
      q.availableCores = q.capacityCores - usedCores
      q
    })
      .sortBy(_.availableCores)(Ordering[Long].reverse).toList

    queueWithCapacity
  }

  def appsBy(user: String, status: String): YarnApps

  def moveApp(app: YarnApp, queues: List[Queue])

}
