package cn.piflow.flow

import cn.piflow.io.{SocketStreamSource, ConsoleSink}
import cn.piflow.processor.ds.{AsDataSet, DoFlatMap, DoMap, DoTransform}
import cn.piflow.processor.io.{DoLoad, DoWrite}
import cn.piflow.util.MockNetCat
import cn.piflow.{FlowGraph, Runner}
import org.apache.spark.sql.execution.streaming.{MemorySink, StreamExecution}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit.{Assert, Test}

class StreamFlowTest {
	val cronExpr = "*/5 * * * * ";
	val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();
	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

	import spark.implicits._

	var nc: MockNetCat = MockNetCat.start(9999);

	@Test
	def testFlowSequence() = {
		val fg = new FlowGraph();
		val node1 = fg.createNode(DoLoad(SocketStreamSource("localhost", 9999)));
		val node2 = fg.createNode(AsDataSet[String]());
		val node3 = fg.createNode(DoMap[String, String](_.toUpperCase()));
		val node4 = fg.createNode(DoFlatMap[String, String](_.split(" ")));
		val node5 = fg.createNode(DoTransform[Dataset[String], DataFrame](_.groupBy("value").count));
		val node6 = fg.createNode(DoWrite(ConsoleSink(), OutputMode.Complete));
		fg.link(node1, node2);
		fg.link(node2, node3);
		fg.link(node3, node4);
		fg.link(node4, node5);
		fg.link(node4, node6);
		fg.show();

		val runner = Runner.sparkRunner(spark);
		runner.schedule(fg);

		nc.writeData("hello\r\nworld\r\nbye\r\nworld\r\n");
		Thread.sleep(10000);

		runner.stop();
	}
}

