package cn.piflow.flow

import java.io.File

import cn.piflow.io._
import cn.piflow.processor.ds.{AsDataSet, DoFlatMap, DoMap, DoTransform}
import cn.piflow.processor.io.{DoLoad, DoWrite}
import cn.piflow.util.MockNetCat
import cn.piflow.{FlowGraph, Runner}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.datasources.{FileFormat, FileFormatWriter}
import org.apache.spark.sql.execution.streaming.http.{HttpStreamClient, HttpStreamServer}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Row, DataFrame, Dataset, SparkSession}
import org.junit.{After, Before, Assert, Test}

class StreamFlowTest {
	val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();
	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

	import spark.implicits._

	private def testFlowSequence(source: StreamSource, sink: StreamSink = MemorySink())(
		generateData: => Unit) = {

		val fg = new FlowGraph();
		val node1 = fg.createNode(DoLoad(source));
		val node2 = fg.createNode(AsDataSet[String]());
		val node3 = fg.createNode(DoMap[String, String](_.toUpperCase()));
		val node4 = fg.createNode(DoFlatMap[String, String](_.split(" ")));
		val node5 = fg.createNode(DoTransform[Dataset[String], DataFrame](_.groupBy("value").count));
		//val node6 = fg.createNode(DoWrite(ConsoleSink(), OutputMode.Complete()));

		val node6 = fg.createNode(DoWrite(sink, OutputMode.Complete()));
		fg.link(node1, node2);
		fg.link(node2, node3);
		fg.link(node3, node4);
		fg.link(node4, node5);
		fg.link(node5, node6);
		fg.show();

		val runner = Runner.sparkRunner(spark);
		runner.schedule(fg);

		generateData;
		Thread.sleep(10000);

		if (sink.isInstanceOf[MemorySink])
			Assert.assertEquals(Seq(Seq("HELLO", 1), Seq("BYE", 1), Seq("WORLD", 2)),
				sink.asInstanceOf[MemorySink].asSeqs);
	}

	@Test
	def testSocketStream(): Unit = {
		val nc: MockNetCat = MockNetCat.start(9999);
		testFlowSequence(
			SocketStreamSource("localhost", 9999)) {
			nc.writeData("hello\r\nworld\r\nbye\r\nworld\r\n");
		};

		nc.stop();
	}

	@Test
	def testHttpStream(): Unit = {
		val receiver = HttpStreamServer.start("/xxxx", 8080);
		receiver.withBuffer()
			.createTopic[String]("topic-1");

		testFlowSequence(
			HttpStreamSource("http://localhost:8080/xxxx", "topic-1")) {
			val producer = HttpStreamClient.connect("http://localhost:8080/xxxx");
			producer.sendRows("topic-1", -1, Array(Row("hello"), Row("world"), Row("bye"), Row("world")));
		};

		receiver.stop();
	}

	@Test
	def testFileSink(): Unit = {
		val nc: MockNetCat = MockNetCat.start(9999);
		FileUtils.deleteDirectory(new File("./target/1.json"));
		testFlowSequence(
			SocketStreamSource("localhost", 9999), FileStreamSink("./target/1.json", FileFormat.JSON)) {
			nc.writeData("hello\r\nworld\r\nbye\r\nworld\r\n");
		};

		nc.stop();
	}
}

