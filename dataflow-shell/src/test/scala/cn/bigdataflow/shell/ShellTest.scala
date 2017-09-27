package cn.bigdataflow.shell;

import org.apache.spark.sql.SparkSession
import org.junit.Test
import cn.bigdataflow.Runner

class ShellTest {
	@Test
	def testFlowSequence() = {
		Shell.run();
	}
}