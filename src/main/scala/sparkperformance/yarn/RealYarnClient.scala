package sparkperformance.yarn

import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper}

import scala.io.Source

class RealYarnClient(yarnUrl: String) extends YarnClient {
  override def clusterMetrics(): ClusterMetrics = {

    val resourceUrl = s"${yarnUrl}/ws/v1/cluster/metrics"
    val content = Source.fromURL(resourceUrl).mkString
    jsonReader().readValue(content, classOf[ClusterMetrics])
  }

  override def clusterQueue(): ClusterScheduler = {
    val resourceUrl = s"${yarnUrl}/ws/v1/cluster/scheduler"
    val content = Source.fromURL(resourceUrl).mkString
    jsonReader().readValue(content, classOf[ClusterScheduler])
  }

  override def appsBy(user: String, status: String): YarnApps = {
    val resourceUrl = s"${yarnUrl}/ws/v1/cluster/apps?user=${user}&states=${status}"
    val content = Source.fromURL(resourceUrl).mkString
    jsonReader().readValue(content, classOf[YarnApps])
  }

  override def moveApp(app: YarnApp, queues: List[Queue]): Unit = {

  }


  private def jsonReader(): ObjectMapper = {
    val json = new ObjectMapper()
    json.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    json
  }
}
