package cn.bigdataflow.runner

import org.quartz.JobExecutionException
import org.quartz.JobExecutionContext
import cn.bigdataflow.Logging
import org.quartz.JobListener

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
