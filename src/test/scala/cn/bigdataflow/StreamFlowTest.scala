package cn.bigdataflow;

import java.io.File
import java.io.PrintWriter
import java.net.Socket

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.junit.After
import org.junit.Before
import org.junit.Test

import cn.bigdataflow.lib.processors.trans.DoFlatMap
import cn.bigdataflow.lib.processors.trans.DoLoadStream
import cn.bigdataflow.lib.processors.trans.DoMap
import cn.bigdataflow.lib.processors.trans.DoTransform
import cn.bigdataflow.lib.processors.trans.DoWriteStream
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.execution.streaming.MemorySink
import org.junit.Assert
import org.apache.spark.streaming.util.MockNetCat

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
		val node1 = fg.createNode(DoLoadStream[String]("socket", Map[String, String]("host" -> "localhost", "port" -> "9999")));
		val node2 = fg.createNode(DoMap[String, String](_.toUpperCase()));
		val node3 = fg.createNode(DoFlatMap[String, String](_.split(" ")));
		val node4 = fg.createNode(DoTransform[String, Row](_.groupBy("value").count));
		val node5 = fg.createNode(DoWriteStream("query1", "memory", OutputMode.Complete()));
		fg.link(node1, node2);
		fg.link(node2, node3);
		fg.link(node3, node4);
		fg.link(node4, node5);
		fg.show();

		FileUtils.deleteDirectory(new File(s"/tmp/query1"));
		val runner = Runner.sparkRunner(spark);
		val ctx = runner.createRunnerContext();
		val t = new Thread() {
			override def run() = {
				runner.run(fg);
			}
		};

		t.start();
		nc.writeData("hello\r\nworld\r\nbye\r\nworld\r\n");
		Thread.sleep(10000);

		val sink = ctx("query1").asInstanceOf[StreamExecution].sink.asInstanceOf[MemorySink];
		val ds = sink.allData;
		Assert.assertArrayEquals(Array("HELLO" -> 1, "BYE" -> 1, "WORLD" -> 2).asInstanceOf[Array[Object]], ds.map(row ⇒ (row(0) -> row(1))).toArray.asInstanceOf[Array[Object]]);
	}
}

