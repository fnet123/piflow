package cn.piflow.shell.cmd

import cn.piflow.shell.Cmd
import cn.piflow.util.FormatUtils
import cn.piflow.{JobInstance, JobManager, Runner, ScheduledJob}

import scala.collection.Seq

class JobCmd(runner: Runner) extends Cmd {
	def list = listJobInstances(None);
	def list(jobId: String): Unit = listJobInstances(Some(jobId));
	def list(jobId: Int): Unit = list("" + jobId);
	def slist(): Unit = listScheduledJobs();
	def hist(): Unit = listJobExecutions();
	def hist(jobId: String): Unit = listJobExecutions(jobId);
	def hist(jobId: Int): Unit = hist("" + jobId);

	private def existingJobId(jm: JobManager, jobId: String): Option[String] = {
		if (!jm.exists(jobId)) {
			println(s"invalid job id: $jobId");
			None;
		}
		else
			Some(jobId);
	}

	def kill(jobId: String) = {
		val jm = runner.getJobManager();
		existingJobId(jm, jobId).foreach { jobId ⇒
			jm.stop(jobId);
			println(s"job killed: id=$jobId");
		}
	}

	def pause(jobId: String) = {
		val jm = runner.getJobManager();
		existingJobId(jm, jobId).foreach { jobId ⇒
			jm.pause(jobId);
			println(s"job paused: id=$jobId");
		}
	}

	def resume(jobId: String) = {
		val jm = runner.getJobManager();
		existingJobId(jm, jobId).foreach { jobId ⇒
			jm.resume(jobId);
			println(s"job paused: id=$jobId");
		}
	}

	private def printInstances(jobs: Seq[JobInstance]) = {
		val data = jobs.map { ji: JobInstance ⇒
			val sj = ji.getScheduledJob();
			Seq(ji.getId(), sj.getId(), FormatUtils.formatDate(ji.getStartTime()), ji.getRunTime());
		}

		FormatUtils.printTable(Seq("id", "sid", "start-time", "run-time"), data, "");
	}

	def listJobInstances(jobId: Option[String]) = {
		val jm = runner.getJobManager();
		if (jobId.isDefined) {
			existingJobId(jm, jobId.get).foreach { jobId ⇒
				printInstances(jm.getRunningJobs(jobId));
			}
		}
		else {
			printInstances(jm.getRunningJobs());
		};
	}

	def listJobExecutions(jobId: String) = {
		val jm = runner.getJobManager();
		printInstances(jm.getHistoricExecutions(jobId));
	}

	def listJobExecutions() = {
		val jm = runner.getJobManager();
		printInstances(jm.getHistoricExecutions());
	}

	def listScheduledJobs() = {
		val jm = runner.getJobManager();
		val data = jm.getScheduledJobs().map { sj: ScheduledJob ⇒
			Seq(sj.getId(), FormatUtils.formatDate(sj.getStartTime()),
				jm.getFireCount(sj.getId()), FormatUtils.formatDate(sj.getPreviousFireTime()),
				FormatUtils.formatDate(sj.getNextFireTime()));
		}

		FormatUtils.printTable(Seq("sid", "start-time", "run-times", "previous-fire", "next-fire"), data, "");
	}
}