package cn.piflow.flow

;

import java.nio.ByteBuffer

import cn.piflow.FlowGraph
import cn.piflow.io.{MemorySink, SeqAsSource}
import cn.piflow.processor.ds.DoMap
import cn.piflow.processor.io.{DoLoad, DoWrite}
import cn.piflow.util.SerDeUtils
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.junit.Test

class SerDeTest {
	val spark = SparkSession.builder.master("local[4]")
		.getOrCreate();
	spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

	import spark.implicits._

	@Test
	def test1() {
		println(SerDeUtils.deserialize[FlowGraph](SerDeUtils.serialize((1 -> "abc"))));
	}

	@Test
	def test2() {
		val fg = new FlowGraph();
		val node1 = fg.createNode(DoLoad(SeqAsSource(1, 2, 3, 4)));
		val node2 = fg.createNode(DoMap[Int, Int](_ + 1));
		val mem = MemorySink();
		val node3 = fg.createNode(DoWrite(mem));
		fg.link(node1, node2, ("out:_1", "in:_1"));
		fg.link(node2, node3, ("out:_1", "in:_1"));
		fg.show();

		val kryo = new KryoSerializer(new SparkConf()).newInstance();
		val bytes = kryo.serialize(fg).array();
		val fg1 = kryo.deserialize(ByteBuffer.wrap(bytes)).asInstanceOf[FlowGraph];
		fg1.show();

		val content = SerDeUtils.serialize(fg);
		val fg2 = SerDeUtils.deserialize[FlowGraph](content);
		fg2.show();
	}

}
