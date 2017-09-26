package cn.bigdataflow;

import org.apache.spark.sql.SparkSession
import cn.bigdataflow.shell.Shell
import org.junit.Test

class ShellTest {
	val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();
	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");
	implicit val runner = Runner.sparkRunner(spark);

	@Test
	def testFlowSequence() = {
		Shell.run();
	}
}

