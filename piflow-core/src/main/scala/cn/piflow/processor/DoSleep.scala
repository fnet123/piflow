package cn.piflow.processor

import cn.piflow.RunnerContext

/**
  * @author bluejoe2008@gmail.com
  *
  *         do nothing, just sleep, this class is for test use
  */
case class DoSleep(sleepTime: Long) extends Processor121 {
  def perform(input: Any, ctx: RunnerContext): Any = {
    Thread.sleep(sleepTime);
    input;
  }
}