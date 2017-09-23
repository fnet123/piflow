package cn.bigdataflow

import java.util.Date

import scala.reflect.ClassTag

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import cn.bigdataflow.runner.SparkRunner

trait Logging {
	protected val logger = Logger.getLogger(this.getClass);
}

trait RunnerContext {
	def apply[T](name: String): T;
	def forType[T: ClassTag](implicit m: Manifest[T]): T =
		apply(m.runtimeClass.getName);

	def update[T](name: String, value: T);
}

trait Runner {
	def getJobManager(): JobManager;
	def run(flowGraph: FlowGraph, scheduler: JobScheduler = JobScheduler.startNow): JobId;
}

object Runner {
	def sparkRunner(spark: SparkSession) = new SparkRunner(spark);
}

trait JobManager {
	def getScheduledJobs(): Seq[ScheduledJob];
	def getRunningJobs(): Seq[JobInstance];
	def getRunningJobs(jobId: JobId): Seq[JobInstance];
	def resume(jobId: JobId);
	def pause(jobId: JobId);
	def stop(jobId: JobId);
}

trait JobInstance {
	def getId(): String;
	def getScheduledJob(): ScheduledJob;
	def getFireTime(): Date;
	def getRunTime(): Long;
	def getRefireCount(): Int;
}

trait ScheduledJob {
	def getFlowGraph(): FlowGraph;
	def getId(): JobId;
	def getNextFireTime(): Date;
	def getStartTime(): Date;
	def getEndTime(): Date;
	def getPreviousFireTime(): Date;
}

trait JobId {
	def getId(): String;
}