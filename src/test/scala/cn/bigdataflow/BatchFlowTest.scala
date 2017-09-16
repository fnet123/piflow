package cn.bigdataflow;

import java.io.File
import java.io.FileWriter

import org.apache.spark.sql.SparkSession
import org.junit.Assert
import org.junit.Test

import cn.bigdataflow.lib.io.MemorySink
import cn.bigdataflow.lib.processors.DoFilter
import cn.bigdataflow.lib.processors.DoFork
import cn.bigdataflow.lib.processors.DoLoad
import cn.bigdataflow.lib.processors.DoMap
import cn.bigdataflow.lib.processors.DoMerge
import cn.bigdataflow.lib.processors.DoZip
import cn.bigdataflow.lib.processors.DoWrite
import cn.bigdataflow.lib.io.SeqAsSource
import cn.bigdataflow.lib.io.ConsoleSink

class BatchFlowTest {
	val cronExpr = "*/5 * * * * ";
	val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();
	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");
	import spark.implicits._

	/*
	def testBatchingFlow() {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();
		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");
		import spark.implicits._

		val flow1 = Flow.batching();
		val node = flow1.load(SeqAsSource(1, 2, 3, 4)).pipe(DoMap((x: Int) ⇒ x + 1)).write(ConsoleSink[Int]());
		val flow2 = Flow.batching();
		flow2.begin(new MyProcessor(collection.immutable.Map[String, String]()));

		val flow3 = Flow.batching();
		val Seq(node3, node4) = flow3.load(SeqAsSource(1, 2, 3, 4)).fork[Int](_ % 2 == 0, _ % 2 == 1);
		node3.write(ConsoleSink[Int]());
		node4.write(ConsoleSink[Int]());

		val runner = Runner.sparkRunner(spark);
		runner.run(flow1);
		runner.schedule(flow2, new Date());
		runner.schedule(flow1, cronExpr);
		runner.run(flow3);
	}
*/

	@Test
	def testFlowSequence() = {
		val fg = new FlowGraph();
		val node1 = fg.createNode(DoLoad(SeqAsSource(1, 2, 3, 4)));
		val node2 = fg.createNode(DoMap[Int, Int](_ + 1));
		val mem = MemorySink[String]();
		val node3 = fg.createNode(DoWrite(mem));
		fg.link(node1, node2, ("out:_1", "in:_1"));
		fg.link(node2, node3, ("out:_1", "in:_1"));
		fg.show();

		val runner = Runner.sparkRunner(spark);
		runner.run(fg);
		Assert.assertEquals(Seq(2, 3, 4, 5), mem.get());
	}

	@Test
	def testLoadDefinedSource() = {
		val fg = new FlowGraph();
		val fw = new FileWriter(new File("./abc.txt"));
		fw.write("hello\r\nworld");
		fw.close();

		val node1 = fg.createNode(DoLoad[String]("text", Map("path" -> "./abc.txt")));
		val mem = MemorySink[String]();
		val node2 = fg.createNode(DoWrite(mem, ConsoleSink()));
		fg.link(node1, node2, ("out:_1", "in:_1"));
		fg.show();

		val runner = Runner.sparkRunner(spark);
		runner.run(fg);
		Assert.assertEquals(Seq("hello", "world"), mem.get());
	}

	@Test
	def testFlowFork() = {
		val fg = new FlowGraph();
		val node1 = fg.createNode(DoLoad(SeqAsSource(1, 2, 3, 4)));
		val node2 = fg.createNode(DoFork[Int](_ % 2 == 0, _ % 2 == 1));
		val mem1 = MemorySink[String]();
		val node3 = fg.createNode(DoWrite(mem1));
		val mem2 = MemorySink[String]();
		val node4 = fg.createNode(DoWrite(mem2));
		fg.link(node1, node2, ("out:_1", "in:_1"));
		fg.link(node2, node3, ("out:_1", "in:_1"));
		fg.link(node2, node4, ("out:_2", "in:_1"));
		fg.show();

		val runner = Runner.sparkRunner(spark);
		runner.run(fg);
		Assert.assertEquals(Seq(2, 4), mem1.get());
		Assert.assertEquals(Seq(1, 3), mem2.get());
	}

	@Test
	def testFlowMerge() = {
		val fg = new FlowGraph();
		val node1 = fg.createNode(DoLoad(SeqAsSource(1, 2, 3, 4)));
		val node2 = fg.createNode(DoLoad(SeqAsSource("a", "b", "c", "d")));
		val node3 = fg.createNode(DoMap[Int, Int](_ + 10));
		val node4 = fg.createNode(DoMap[String, String](_.toUpperCase()));
		//merge
		val node5 = fg.createNode(DoZip[Int, String]());
		val mem = MemorySink[String]();
		val node6 = fg.createNode(DoWrite(mem, ConsoleSink[String]()));
		fg.link(node1, node3, ("out:_1", "in:_1"));
		fg.link(node2, node4, ("out:_1", "in:_1"));
		fg.link(node3, node5, ("out:_1", "in:_1"));
		fg.link(node4, node5, ("out:_1", "in:_2"));
		fg.link(node5, node6, ("out:_1", "in:_1"));
		fg.show();

		val runner = Runner.sparkRunner(spark);
		runner.run(fg);
		Assert.assertEquals(Seq((11, "A"), (12, "B"), (13, "C"), (14, "D")), mem.get());
	}

	@Test
	def testFlowForkMerge() = {
		val fg = new FlowGraph();
		val node1 = fg.createNode(DoLoad(SeqAsSource(1, 2, 3, 4)));
		val node2 = fg.createNode(DoLoad(SeqAsSource("a", "b", "c", "d")));
		val node3 = fg.createNode(DoMap[String, String](_.toUpperCase()));
		//fork
		val node4 = fg.createNode(DoFork[Int](_ % 2 == 0, _ % 2 == 1));

		val node5 = fg.createNode(DoMap[Int, String](x ⇒ (x + 10).toString()));
		val node6 = fg.createNode(DoFilter[String](_.charAt(0) <= 'B'));
		//merge
		val node7 = fg.createNode(DoMerge[String]());
		val mem = MemorySink[String]();
		val node8 = fg.createNode(DoWrite(mem, ConsoleSink[String]()));
		fg.link(node1, node4, ("out:_1", "in:_1"));
		fg.link(node4, node5, ("out:_1", "in:_1"));
		fg.link(node2, node3, ("out:_1", "in:_1"));
		fg.link(node3, node6, ("out:_1", "in:_1"));
		fg.link(node5, node7, ("out:_1", "in:_1"));
		fg.link(node6, node7, ("out:_1", "in:_2"));
		fg.link(node7, node8, ("out:_1", "in:_1"));
		//node2 is isolated
		fg.show();

		val runner = Runner.sparkRunner(spark);
		runner.run(fg);

		Assert.assertEquals(Seq("12", "14", "A", "B"), mem.get());
	}
}

