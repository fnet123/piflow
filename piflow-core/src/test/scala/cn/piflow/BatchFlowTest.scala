package cn.piflow

import java.io.{File, FileWriter}
import cn.piflow.io.{MemorySink, SeqAsSource}
import cn.piflow.processor.ds.{DoFilter, DoFork, DoMap, DoMerge, DoZip}
import cn.piflow.processor.io.{DoLoad, DoWrite}
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class BatchFlowTest {
  val cronExpr = "*/5 * * * * ";
  val spark = SparkSession.builder.master("local[4]")
    .getOrCreate();
  spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp");
  import spark.implicits._

  @Test
  def testFlowSequence() = {
    val fg = new FlowGraph();
    val node1 = fg.createNode(DoLoad(SeqAsSource(1, 2, 3, 4)));
    val node2 = fg.createNode(DoMap[Int, Int](_ + 1));
    val mem = MemorySink();
    val node3 = fg.createNode(DoWrite(mem));
    fg.link(node1, node2, ("out:_1", "in:_1"));
    fg.link(node2, node3, ("out:_1", "in:_1"));
    fg.show();

    val runner = Runner.sparkRunner(spark);
    runner.run(fg);
    Assert.assertEquals(Seq(2, 3, 4, 5), mem.as[Int]);
  }

  @Test
  def testLoadDefinedSource() = {
    val fg = new FlowGraph();
    val fw = new FileWriter(new File("./abc.txt"));
    fw.write("hello\r\nworld");
    fw.close();

    val node1 = fg.createNode(DoLoad("text", Map("path" -> "./abc.txt")));
    val mem = MemorySink();
    val node2 = fg.createNode(DoWrite(mem));
    fg.link(node1, node2, ("out:_1", "in:_1"));
    fg.show();

    val runner = Runner.sparkRunner(spark);
    runner.run(fg);
    Assert.assertEquals(Seq("hello", "world"), mem.as[String]);
  }

  @Test
  def testFlowFork() = {
    val fg = new FlowGraph();
    val node1 = fg.createNode(DoLoad(SeqAsSource(1, 2, 3, 4)));
    val node2 = fg.createNode(DoFork[Int](_ % 2 == 0, _ % 2 == 1));
    val mem1 = MemorySink();
    val node3 = fg.createNode(DoWrite(mem1));
    val mem2 = MemorySink();
    val node4 = fg.createNode(DoWrite(mem2));
    fg.link(node1, node2, ("out:_1", "in:_1"));
    fg.link(node2, node3, ("out:_1", "in:_1"));
    fg.link(node2, node4, ("out:_2", "in:_1"));
    fg.show();

    val runner = Runner.sparkRunner(spark);
    runner.run(fg);
    Assert.assertEquals(Seq(2, 4), mem1.as[Int]);
    Assert.assertEquals(Seq(1, 3), mem2.as[Int]);
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
    val mem = MemorySink();
    val node6 = fg.createNode(DoWrite(mem));
    fg.link(node1, node3, ("out:_1", "in:_1"));
    fg.link(node2, node4, ("out:_1", "in:_1"));
    fg.link(node3, node5, ("out:_1", "in:_1"));
    fg.link(node4, node5, ("out:_1", "in:_2"));
    fg.link(node5, node6, ("out:_1", "in:_1"));
    fg.show();

    val runner = Runner.sparkRunner(spark);
    runner.run(fg);
    Assert.assertEquals(Seq(Seq(11, "A"), Seq(12, "B"), Seq(13, "C"), Seq(14, "D")), mem.asSeq);
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
    val mem = MemorySink();
    val node8 = fg.createNode(DoWrite(mem));
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

    Assert.assertEquals(Seq("12", "14", "A", "B"), mem.as[String]);
  }
}

