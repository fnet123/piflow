package cn.bigdataflow.runner

import java.util.Date

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.quartz.JobDetail
import org.quartz.JobExecutionContext
import org.quartz.JobKey
import org.quartz.Scheduler
import org.quartz.Trigger
import org.quartz.TriggerKey

import cn.bigdataflow.FlowGraph
import cn.bigdataflow.JobId
import cn.bigdataflow.JobInstance
import cn.bigdataflow.JobManager
import cn.bigdataflow.ScheduledJob
import org.quartz.impl.matchers.GroupMatcher

class FlowGraphJobManager(scheduler: Scheduler) extends JobManager {
	def getScheduledJobs(): Seq[ScheduledJob] = {
		scheduler.getTriggerKeys(GroupMatcher.groupEquals(classOf[FlowGraph].getName)).map { tk: TriggerKey ⇒
			val trigger = scheduler.getTrigger(tk);
			val jobDetail = scheduler.getJobDetail(trigger.getJobKey);
			new SimpleScheduledJob(jobDetail, trigger);
		}.toSeq
	}

	def getRunningJobs(): Seq[JobInstance] = {
		scheduler.getCurrentlyExecutingJobs.map { ctx ⇒
			new SimpleJobInstance(ctx);
		}.toSeq
	}

	def getRunningJobs(jobId: JobId): Seq[JobInstance] = {
		getRunningJobs().filter { job ⇒
			jobId.getId().eq(job.getScheduledJob().getId().getId())
		}
	}

	def resume(jobId: JobId) = {
		scheduler.resumeTrigger(jobId.asInstanceOf[SimpleScheduledJob].trigger.getKey);
	}

	def pause(jobId: JobId) = {
		scheduler.pauseTrigger(jobId.asInstanceOf[SimpleScheduledJob].trigger.getKey);
	}

	def stop(jobId: JobId) = {
		scheduler.unscheduleJob(jobId.asInstanceOf[SimpleScheduledJob].trigger.getKey);
	}
}

case class SimpleJobInstance(ctx: JobExecutionContext) extends JobInstance {
	def getId(): String = ctx.getFireInstanceId;
	def getScheduledJob(): SimpleScheduledJob = new SimpleScheduledJob(ctx.getJobDetail, ctx.getTrigger);
	def getFireTime(): Date = ctx.getFireTime;
	def getRunTime(): Long = ctx.getJobRunTime;
	def getRefireCount(): Int = ctx.getRefireCount;
}

case class SimpleJobId(jobDetailKey: JobKey, triggerKey: TriggerKey) extends JobId {
	def getId = triggerKey.getName;
}

case class SimpleScheduledJob(jobDetail: JobDetail, trigger: Trigger) extends ScheduledJob {
	def getFlowGraph(): FlowGraph = jobDetail.getJobDataMap().get(classOf[FlowGraph].getName).asInstanceOf[FlowGraph];
	def getId(): SimpleJobId = new SimpleJobId(jobDetail.getKey, trigger.getKey);
	def getNextFireTime(): Date = trigger.getNextFireTime;
	def getStartTime(): Date = trigger.getStartTime;
	def getEndTime(): Date = trigger.getStartTime;
	def getPreviousFireTime(): Date = trigger.getPreviousFireTime;
}