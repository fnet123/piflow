package cn.piflow.shell

import java.util.Date

import cn.piflow._
import cn.piflow.dsl.BoundNode
import cn.piflow.util.FormatUtils
import cn.piflow.dsl._

/**
  * @author bluejoe2008@gmail.com
  */

class RunnableFlowGraph(flowGraph: FlowGraph)(implicit runner: Runner) {
  def this(node: ChainWithTail[_])(implicit runner: Runner) =
    this(asGraph(node))(runner);

  def !() {
    val time1 = +System.currentTimeMillis();
    val job = runner.run(flowGraph);
    val time2 = System.currentTimeMillis();
    val jobId = job.getId();
    val cost = time2 - time1;
    println(s"job complete: id=$jobId, time cost=${cost}ms");
  }

  def &() {
    val job = runner.schedule(flowGraph);
    val jobId = job.getId();
    println(s"job scheduled: id=$jobId");
  }

  def !@(date: Date) = {
    val job = runner.schedule(flowGraph, Start.at(date));
    printScheduledJobInfo(job);
  }

  def !@(schedule: JobSchedule) = {
    val job = runner.schedule(flowGraph, schedule);
    printScheduledJobInfo(job);
  }

  def !@(start: Start.Builder = Start.now, repeat: Repeat.Builder = Repeat.once) = {
    val job = runner.schedule(flowGraph, start, repeat);
    printScheduledJobInfo(job);
  }

  def !@(cronExpression: String) = {
    val job = runner.schedule(flowGraph, Start.now, Repeat.cronedly(cronExpression));
    printScheduledJobInfo(job);
  }

  private def printScheduledJobInfo(job: ScheduledJob) = {
    val jobId = job.getId();
    val nftime = FormatUtils.formatDate(job.getNextFireTime());
    println(s"job scheduled: id=$jobId, next fired time=$nftime");
  }

  def !@(delay: Long) = {
    val job = runner.schedule(flowGraph, Start.later(delay));
    printScheduledJobInfo(job);
  }
}