package cn.piflow

import java.util.Date

import org.quartz.{CronScheduleBuilder, ScheduleBuilder, SimpleScheduleBuilder}

class JobSchedule() {
  var scheduleBuilder: Option[ScheduleBuilder[_]] = None;
  var startTime: Option[Date] = None;

  def runOnce() = {
    scheduleBuilder = None;
    this;
  }

  def repeatCronedly(cronExpression: String) = {
    scheduleBuilder = Some(CronScheduleBuilder.cronSchedule(cronExpression));
    this;
  }

  def repeatWithInterval(interval: Long, repeatCount: Int = -1) = {
    val ssb = SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(interval);
    if (repeatCount < 0)
      ssb.repeatForever();
    else
      ssb.withRepeatCount(repeatCount);

    scheduleBuilder = Some(ssb);
    this;
  }

  def repeatDaily(hour: Int, minute: Int) = {
    scheduleBuilder = Some(CronScheduleBuilder.dailyAtHourAndMinute(hour, minute));
    this;
  }

  def repeatMonthly(dayOfMonth: Int, hour: Int, minute: Int) = {
    scheduleBuilder = Some(CronScheduleBuilder.monthlyOnDayAndHourAndMinute(dayOfMonth, hour, minute));
    this;
  }

  def repeatWeekly(dayOfWeek: Int, hour: Int, minute: Int) = {
    scheduleBuilder = Some(CronScheduleBuilder.weeklyOnDayAndHourAndMinute(dayOfWeek, hour, minute));
    this;
  }

  def startNow() = {
    startTime = None;
    this;
  }

  def startAt(date: Date) = {
    startTime = Some(date);
    this;
  }

  def startLater(delay: Long) = {
    startTime = Some(new Date(System.currentTimeMillis() + delay));
    this;
  }

  //startAfter(ScheduledJob)
}

object Start {
  type Builder = (JobSchedule) => Unit;

  def now: Builder = { schedule: JobSchedule =>
    schedule.startNow();
  }

  def at(date: Date): Builder = { schedule: JobSchedule =>
    schedule.startAt(date);
  }

  def later(delay: Long): Builder = { schedule: JobSchedule =>
    schedule.startLater(delay);
  }
}

object Repeat {
  type Builder = (JobSchedule) => Unit;

  def cronedly(cronExpression: String): Builder = { schedule: JobSchedule =>
    schedule.repeatCronedly(cronExpression);
  }

  def once: Builder = { schedule: JobSchedule =>
    schedule.runOnce();
  };

  def periodically(interval: Long, repeatCount: Int = -1): Builder = { schedule: JobSchedule =>
    schedule.repeatWithInterval(interval, repeatCount);
  }

  def daily(hour: Int, minute: Int): Builder = { schedule: JobSchedule =>
    schedule.repeatDaily(hour, minute);
  }

  def monthly(dayOfMonth: Int, hour: Int, minute: Int): Builder = { schedule: JobSchedule =>
    schedule.repeatMonthly(dayOfMonth, hour, minute);
  }

  def weekly(dayOfWeek: Int, hour: Int, minute: Int): Builder = { schedule: JobSchedule =>
    schedule.repeatWeekly(dayOfWeek, hour, minute);
  }
}