package cn.piflow.runner

import java.util.Date

import cn.piflow.{FlowGraph, JobInstance, JobManager, ScheduledJob}
import org.quartz.impl.matchers.GroupMatcher
import org.quartz.{JobDetail, JobExecutionContext, Scheduler, Trigger, TriggerKey}

import scala.collection.JavaConversions.iterableAsScalaIterable

class JobManagerImpl(scheduler: Scheduler, teg: TriggerExtraGroup) extends JobManager {
  def getFireCount(jobId: String): Int = {
    teg.get(jobId).getFireCount();
  }

  def exists(jobId: String): Boolean = {
    scheduler.getTrigger(jobId2TriggerKey(jobId)) != null;
  }

  def getHistoricExecutions(jobId: String) = {
    teg.getHistoricExecutions().filter(_.getTrigger.getKey.getName.equals(jobId)).map {
      new JobInstanceImpl(_);
    }
  }

  def getHistoricExecutions() = {
    teg.getHistoricExecutions().map {
      new JobInstanceImpl(_);
    }
  }

  def getScheduledJobs(): Seq[ScheduledJob] = {
    scheduler.getTriggerKeys(GroupMatcher.groupEquals(classOf[FlowGraph].getName)).map { tk: TriggerKey ⇒
      val trigger = scheduler.getTrigger(tk);
      val jobDetail = scheduler.getJobDetail(trigger.getJobKey);
      new ScheduledJobImpl(jobDetail, trigger);
    }.toSeq
  }

  def getRunningJobs(jobId: String): Seq[JobInstance] = {
    getRunningJobs().filter { job ⇒
      jobId.eq(job.getScheduledJob().getId())
    }
  }

  def getRunningJobs(): Seq[JobInstance] = {
    scheduler.getCurrentlyExecutingJobs.map { ctx ⇒
      new JobInstanceImpl(ctx);
    }.toSeq
  }

  def resume(jobId: String) = {
    scheduler.resumeTrigger(jobId2TriggerKey(jobId));
  }

  private def jobId2TriggerKey(jobId: String) = new TriggerKey(jobId, classOf[FlowGraph].getName);

  def pause(jobId: String) = {
    scheduler.pauseTrigger(jobId2TriggerKey(jobId));
  }

  def stop(jobId: String) = {
    scheduler.unscheduleJob(jobId2TriggerKey(jobId));
  }
}

case class JobInstanceImpl(ctx: JobExecutionContext) extends JobInstance {
  def getId(): String = ctx.getFireInstanceId;

  def getScheduledJob(): ScheduledJobImpl = new ScheduledJobImpl(ctx.getJobDetail, ctx.getTrigger);

  def getStartTime(): Date = ctx.getFireTime;

  def getRunTime(): Long = ctx.getJobRunTime;

  def getRefireCount(): Int = ctx.getRefireCount;
}

case class ScheduledJobImpl(jobDetail: JobDetail, trigger: Trigger) extends ScheduledJob {
  def getFlowGraph(): FlowGraph = jobDetail.getJobDataMap().get(classOf[FlowGraph].getName).asInstanceOf[FlowGraph];

  def getId(): String = trigger.getKey.getName;

  def getNextFireTime(): Date = trigger.getNextFireTime;

  def getStartTime(): Date = trigger.getStartTime;

  def getEndTime(): Date = trigger.getStartTime;

  def getPreviousFireTime(): Date = trigger.getPreviousFireTime;
}