package cn.piflow

import java.util.Date

import org.quartz.{CronScheduleBuilder, ScheduleBuilder, SimpleScheduleBuilder}

object Schedule {
	def startNow() = new Schedule().startNow();
	def startAt(date: Date) = new Schedule().startAt(date);
	def startLater(delay: Long) = new Schedule().startLater(delay);
}

class Schedule() {
	var scheduleBuilder: Option[ScheduleBuilder[_]] = None;
	var startTime: Option[Date] = None;

	def runOnce() = {
		scheduleBuilder = None;
		this;
	}

	def repeatCronly(cronExpression: String) = {
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
}