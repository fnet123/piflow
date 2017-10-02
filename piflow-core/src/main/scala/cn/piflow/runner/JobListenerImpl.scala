package cn.piflow.runner

import cn.piflow.Logging
import org.quartz.{JobExecutionContext, JobExecutionException, JobListener}

/**
  * @author bluejoe2008@gmail.com
  */
class JobListenerImpl() extends JobListener with Logging {
  def getName() = this.getClass.getName;

  def jobToBeExecuted(context: JobExecutionContext) {
    logger.debug(String.format("executing job: %s, scheduledJob: %s", context.getFireInstanceId, context.getTrigger.getKey.getName));
  }

  def jobExecutionVetoed(context: JobExecutionContext) {
  }

  def jobWasExecuted(context: JobExecutionContext,
                     jobException: JobExecutionException) {
    logger.debug(String.format("job executed: %s, scheduledJob: %s", context.getFireInstanceId, context.getTrigger.getKey.getName));
  }
}
