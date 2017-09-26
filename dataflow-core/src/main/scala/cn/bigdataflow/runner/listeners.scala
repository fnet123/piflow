package cn.bigdataflow.runner

import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

import org.quartz.JobDetail
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.quartz.JobKey
import org.quartz.JobListener
import org.quartz.SchedulerException
import org.quartz.SchedulerListener
import org.quartz.Trigger
import org.quartz.Trigger.CompletedExecutionInstruction
import org.quartz.TriggerKey
import org.quartz.TriggerListener

import cn.bigdataflow.Logging

/**
 * @author bluejoe2008@gmail.com
 */
class FlowGraphJobListener extends JobListener with Logging {
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

class FlowGraphJobTriggerListener extends TriggerListener with Logging {
	def getName() = this.getClass.getName;

	def triggerFired(trigger: Trigger, context: JobExecutionContext) = {
		logger.debug(String.format("job fired: %s, scheduledJob: %s", context.getFireInstanceId, trigger.getKey.getName));
	}

	def vetoJobExecution(trigger: Trigger, context: JobExecutionContext) = {
		false;
	}

	def triggerMisfired(trigger: Trigger) = {
	}

	def triggerComplete(trigger: Trigger, context: JobExecutionContext,
		triggerInstructionCode: CompletedExecutionInstruction) {
		logger.debug(String.format("job completed: %s, scheduledJob: %s", context.getFireInstanceId, trigger.getKey.getName));
	}
}

class FlowGraphJobSchedulerListener() extends SchedulerListener with Logging {

	val jobLocks = collection.mutable.Map[String, Object]();

	def getLock(triggerKey: TriggerKey) = {
		jobLocks(triggerKey.getName);
	}

	def jobScheduled(trigger: Trigger) {
		logger.debug(String.format("job scheduled: %s", trigger.getKey.getName));
		val lock = new Object();
		jobLocks(trigger.getKey.getName) = new Object;
	}

	def jobUnscheduled(triggerKey: TriggerKey) {
		logger.debug(String.format("job unscheduled: %s", triggerKey.getName));
	}

	def triggerFinalized(trigger: Trigger) {
		logger.debug(String.format("job finalized: %s", trigger.getKey.getName));
		val lock = jobLocks(trigger.getKey.getName);
		lock.synchronized {
			lock.notify();
		}
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