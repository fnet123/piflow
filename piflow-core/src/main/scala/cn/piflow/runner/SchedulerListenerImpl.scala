package cn.piflow.runner

import cn.piflow.Logging
import org.quartz.{JobDetail, JobKey, SchedulerException, SchedulerListener, Trigger, TriggerKey}

/**
  * @author bluejoe2008@gmail.com
  */
class SchedulerListenerImpl(teg: TriggerExtraGroup) extends SchedulerListener with Logging {
  def jobScheduled(trigger: Trigger) {
    teg.getOrCreate(trigger.getKey);
    logger.debug(String.format("job scheduled: %s", trigger.getKey.getName));
  }

  def jobUnscheduled(triggerKey: TriggerKey) {
    logger.debug(String.format("job unscheduled: %s", triggerKey.getName));
  }

  def triggerFinalized(trigger: Trigger) {
    teg.get(trigger.getKey).notifyTermination();
    logger.debug(String.format("job finalized: %s", trigger.getKey.getName));
    //do not logout: historic records
    //teg.logout(trigger.getKey);
  }

  def triggerPaused(triggerKey: TriggerKey) {}

  def triggersPaused(triggerGroup: String) {}

  def triggerResumed(triggerKey: TriggerKey) {}

  def triggersResumed(triggerGroup: String) {}

  def jobAdded(jobDetail: JobDetail) {}

  def jobDeleted(jobKey: JobKey) {}

  def jobPaused(jobKey: JobKey) {}

  def jobsPaused(jobGroup: String) {}

  def jobResumed(jobKey: JobKey) {}

  def jobsResumed(jobGroup: String) {}

  def schedulerError(msg: String, cause: SchedulerException) {}

  def schedulerInStandbyMode() {}

  def schedulerStarted() {}

  def schedulerStarting() {}

  def schedulerShutdown() {}

  def schedulerShuttingdown() {}

  def schedulingDataCleared() {}
}