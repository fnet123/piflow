import java.util._

import cn.piflow._
import cn.piflow.dsl._
import cn.piflow.io._
import cn.piflow.processor._
import cn.piflow.processor.ds._
import cn.piflow.shell._
import org.apache.spark.sql._

object PRELOAD_CODES {
  implicit val spark = SparkSession.builder.master("local[4]")
    .getOrCreate();

  spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

  import spark.implicits._;

  implicit val runner = Runner.sparkRunner(spark);
  val jobs = new cn.piflow.shell.cmd.JobCmd(runner);
  val store = new cn.piflow.shell.cmd.StoreCmd(runner);

  private def TEST_IMPORTS_WILL_NEVER_USED() {
    if (false) {
      val piped: PipedProcessorNode = SeqAsSource(1, 2, 3, 4);
      toGraph(piped);
      new Date();
      val pl = SeqAsSource(1, 2, 3, 4) > DoMap[Int, Int](_ + 1) > DoSleep(30000) > ConsoleSink();
      pl !;
      pl !@ Schedule.startNow()
    }
  }
}