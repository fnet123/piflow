import cn.piflow.processor.ds._
import cn.piflow.processor.io._
import cn.piflow.io._
import cn.piflow.dsl._
import java.util._
import org.apache.spark.sql._
import cn.piflow._
import cn.piflow.shell._

object PRELOAD_CODES {
	implicit val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();

	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");
	import spark.implicits._;

	implicit val runner = Runner.sparkRunner(spark);
	val jobs = new cn.piflow.shell.cmd.JobCmd(runner);
	val store = new cn.piflow.shell.cmd.StoreCmd(runner);
}