package cn.bigdataflow;

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.junit.Test

import cn.bigdataflow.lib.processors.DoFlatMap
import cn.bigdataflow.lib.processors.DoLoadStream
import cn.bigdataflow.lib.processors.DoMap
import cn.bigdataflow.lib.processors.DoTransform
import cn.bigdataflow.lib.processors.DoWriteStream

class StreamFlowTest {
	val cronExpr = "*/5 * * * * ";
	val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();
	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");
	import spark.implicits._

	@Test
	def testFlowSequence() = {
		val fg = new FlowGraph();
		val node1 = fg.createNode(DoLoadStream[String]("socket", Map[String, String]("host" -> "localhost", "port" -> "9999")));
		val node2 = fg.createNode(DoMap[String, String](_.toUpperCase()));
		val node3 = fg.createNode(DoFlatMap[String, String](_.split(" ")));
		val node4 = fg.createNode(DoTransform[String, Row](_.groupBy("value").count));
		val node5 = fg.createNode(DoWriteStream("query1", "console", OutputMode.Complete()));
		fg.link(node1, node2);
		fg.link(node2, node3);
		fg.link(node3, node4);
		fg.link(node4, node5);
		fg.show();

		FileUtils.deleteDirectory(new File(s"/tmp/query1"));
		val runner = Runner.sparkRunner(spark);
		runner.run(fg);
	}
}

