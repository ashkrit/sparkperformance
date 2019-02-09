package sparkperformance.yarn

import scala.beans.BeanProperty

class ClusterMetrics {

  @BeanProperty
  var clusterMetrics: MetricsDetails = _
}


class MetricsDetails {

  @BeanProperty
  var availableMB: Long = _
  @BeanProperty
  var allocatedVirtualCores: Long = _
  @BeanProperty
  var totalVirtualCores: Long = _
}


class ClusterScheduler {

  @BeanProperty
  var scheduler: ClusterSchedulerDetails = _
}

class ClusterSchedulerDetails {
  @BeanProperty
  var schedulerInfo: SchedulerInfo = _
}


class SchedulerInfo {

  @BeanProperty
  var capacity: Float = _
  @BeanProperty
  var maxCapacity: Float = _

  @BeanProperty
  var queueName: String = _

  @BeanProperty
  var queues: Queues = _

}

class Queues {

  @BeanProperty
  var queue: java.util.List[Queue] = _
}

class Queue {

  @BeanProperty
  var queueName: String = _
  @BeanProperty
  var capacity: Double = _
  @BeanProperty
  var usedCapacity: Double = _
  @BeanProperty
  var maxCapacity: Double = _
  @BeanProperty
  var resourcesUsed: ResourceUsed = _


  //Calculated capacity

  var capacityCores: Long = _
  var availableCores: Long = _
}

class ResourceUsed {

  @BeanProperty
  var memory: Long = _

  @BeanProperty
  var vCores: Long = _

}

class YarnApps {

  @BeanProperty
  var apps: YarnAppsDetail = _
}

class YarnAppsDetail {
  @BeanProperty
  var app: java.util.List[YarnApp] = _
}

class YarnApp {

  @BeanProperty
  var id: String = _
  @BeanProperty
  var user: String = _

  @BeanProperty
  var queue: String = _

  @BeanProperty
  var state: String = _

  @BeanProperty
  var trackingUrl: String = _

  @BeanProperty
  var startedTime: Long = _

  @BeanProperty
  var finishedTime: Long = _
}


