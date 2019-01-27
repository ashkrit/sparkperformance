package sparkperformance.poormonitor

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.slf4j.LoggerFactory

class StageAccumulatorListener extends SparkListener {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    log.info(s"Stage accumulator values:${event.stageInfo.name}")
    event.stageInfo.accumulables.foreach { case (id, accInfo) =>
      log.info(s"$id:${accInfo.name}:${accInfo.value}")
    }
  }

}
