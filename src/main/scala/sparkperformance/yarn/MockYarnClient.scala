package sparkperformance.yarn


import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}

import scala.io.Source

class MockYarnClient extends YarnClient {

  override def clusterMetrics(): ClusterMetrics = {

    val path = this.getClass.getResource("/yarn/clustermetrics.json")
    require(path != null)
    val fileContent = Source.fromFile(path.toURI).getLines().mkString

    val reader = jsonReader

    val metrics = reader.readValue(fileContent, classOf[ClusterMetrics])
    println(s"Cores ${metrics.clusterMetrics.allocatedVirtualCores} , Memory : ${metrics.clusterMetrics.availableMB}")

    metrics
  }

  private def jsonReader(): ObjectMapper = {
    val json = new ObjectMapper()
    json.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    json
  }

  override def clusterQueue(): ClusterScheduler = {

    val path = this.getClass.getResource("/yarn/clusterscheduler.json")
    require(path != null)
    val fileContent = Source.fromFile(path.toURI).getLines().mkString

    val reader = jsonReader()
    val metrics = reader.readValue(fileContent, classOf[ClusterScheduler])
    println(s"Cores ${metrics.scheduler.schedulerInfo.queueName}")

    metrics

  }


  override def moveApp(app: YarnApp, queues: List[Queue]): Unit = {

    val queueOptions = queues
      .filter(q => !q.queueName.equals(app.queue))
      .take(1)

    if (queueOptions.isEmpty) {
      println("No queue available")
    }
    else {
      val targetQueue = queueOptions(0).getQueueName
      println(s"Moving ${app.id} application from ${app.queue} -> ${targetQueue}")
    }

  }

  override def appsBy(user: String, status: String): YarnApps = {

    val path = this.getClass.getResource("/yarn/apps.json")
    require(path != null)
    val fileContent = Source.fromFile(path.toURI).getLines().mkString
    val reader = jsonReader()
    val apps = reader.readValue(fileContent, classOf[YarnApps])
    apps

  }
}

