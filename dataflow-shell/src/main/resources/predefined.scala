object PRELOAD_CODES {
	import cn.bigdataflow.processor.transform._
	import cn.bigdataflow.io._
	import cn.bigdataflow.dsl._
	import java.util._
	import org.apache.spark.sql._
	import cn.bigdataflow._
	import cn.bigdataflow.shell._

	implicit val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();

	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");
	import spark.implicits._;

	implicit val runner = Runner.sparkRunner(spark);
	val jobs = new cn.bigdataflow.shell.cmd.JobCmd(runner);
	val store = new cn.bigdataflow.shell.cmd.StoreCmd(runner);
}