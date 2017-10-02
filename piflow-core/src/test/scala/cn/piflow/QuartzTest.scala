package cn.piflow

;

import java.util.Date

import org.junit.Test
import org.quartz.impl.StdSchedulerFactory
import org.quartz.{CronScheduleBuilder, JobBuilder, JobExecutionContext, TriggerBuilder}

class HelloQuartz extends org.quartz.Job {
  println("=====new instance=====");

  def execute(context: JobExecutionContext) {
    val detail = context.getJobDetail();
    val name = detail.getJobDataMap().getString("name");
    System.out.println("say hello to " + name + " at " + new Date() + "-" + detail.getJobDataMap().get("test"));
    Thread.sleep(2000);
  }
}

class QuartzTest {
  @Test
  def test1() = {
    //创建scheduler
    val scheduler = StdSchedulerFactory.getDefaultScheduler();

    //定义一个Trigger
    val trigger = TriggerBuilder.newTrigger() //.withIdentity("trigger1", "group1") //定义name/group
      .startNow() //一旦加入scheduler，立即生效
      .withSchedule(CronScheduleBuilder.cronSchedule("* * * * * ?") //使用SimpleTrigger
    ) //一直执行，奔腾到老不停歇
      .build();

    //定义一个JobDetail
    val job = JobBuilder.newJob(classOf[HelloQuartz]) //定义Job类为HelloQuartz类，这是真正的执行逻辑所在
      //.withIdentity("job1", "group1") //定义name/group
      .usingJobData("name", "quartz") //定义属性
      .build();

    job.getJobDataMap.put("test", new Object());

    //加入这个调度
    scheduler.scheduleJob(job, trigger);

    //启动之
    println(new Date());
    scheduler.start();

    //运行一段时间后关闭
    Thread.sleep(3000);
    val jd = job;
    val tr = trigger;
    println(scheduler.getCurrentlyExecutingJobs);
    println(tr.getKey);
    println(scheduler.getTriggerState(tr.getKey));
    println(tr.getNextFireTime);
    Thread.sleep(3000);
    scheduler.shutdown(true);
  }
}

