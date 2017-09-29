package cn.piflow.util

import java.nio.ByteBuffer
import java.util.Base64

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.google.common.graph.DirectedGraphConnections
import com.esotericsoftware.kryo.io.Output
import com.google.common.graph.DirectedGraphConnections
import cn.piflow.FlowGraph

object SerDeUtils {
	val kryo = new KryoSerializer(new SparkConf()).newInstance();

	def serialize[T: ClassTag](t: T): String = {
		val bytes = kryo.serialize(t).array();
		new String(Base64.getEncoder.encode(bytes));
	}

	def deserialize[T](content: String): T = {
		val bytes = Base64.getDecoder.decode(content.getBytes);
		kryo.deserialize(ByteBuffer.wrap(bytes)).asInstanceOf[T];
	}

	def graph2Json(graph: FlowGraph): String = {
		//using gson
		"TODO"
	}

	def json2Graph(content: String): FlowGraph = {
		null;
	}
}