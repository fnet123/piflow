package cn.bigdataflow.shell

import cn.bigdataflow.Runner
import cn.bigdataflow.JobInstance
import cn.bigdataflow.util.TablePrinter
import cn.bigdataflow.ScheduledJob

object Cmd {
	def js(implicit runner: Runner) = listJobInstances(runner);
	def sjs(implicit runner: Runner) = listScheduledJobs(runner);

	def listJobInstances(implicit runner: Runner) = {
		val jm = runner.getJobManager();
		val data = jm.getRunningJobs().map { ji: JobInstance ⇒
			val sj = ji.getScheduledJob();
			Seq(ji.getId(), sj.getId().getId(), ji.getStartTime(), ji.getRunTime());
		}

		TablePrinter.print(Seq("id", "sid", "start-time", "run-time"), data, "");
	}

	def listScheduledJobs(implicit runner: Runner) = {
		val jm = runner.getJobManager();
		val data = jm.getScheduledJobs().map { sj: ScheduledJob ⇒
			Seq(sj.getId().getId(), sj.getStartTime(), jm.getFireCount(sj.getId()), sj.getPreviousFireTime(), sj.getNextFireTime());
		}

		TablePrinter.print(Seq("sid", "start-time", "run-times", "previous-fire", "next-fire"), data, "");
	}
}