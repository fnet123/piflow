package cn.piflow.dsl

import cn.piflow.FlowNode
import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor
import cn.piflow.processor.io.{DoLoad, DoWrite}
import org.apache.spark.sql.streaming.OutputMode

import scala.collection.mutable.ArrayBuffer

/**
	* Created by bluejoe on 2017/10/1.
	*/
abstract class BoundNode[T](val userData: T)
	extends Chaining[T] {
	type ArrowType <: Arrow[T];
	type RefType <: Ref[T];
	val successors = ArrayBuffer[Relationship]();
	val predecessors = ArrayBuffer[Relationship]();
	private val refs = ArrayBuffer[RefType]();

	def createProcessor(): Processor;

	def notifyRefs(node: FlowNode): Unit = {
		refs.foreach(_.bind(node));
	}

	def addSuccessor(target: BoundNode[_], ports: (String, String)): Unit = {
		successors += Relationship(target, ports);
	}

	def addPredecessor(target: BoundNode[_], ports: (String, String)): Unit = {
		predecessors += Relationship(target, ports);
	}

	lazy val defaultArrow = createArrow("_1" -> "_1");

	def createArrow(ports: (String, String)): ArrowType;

	def tail(): Arrow[T] = defaultArrow;

	def %(ref: RefType): this.type = as(ref);

	def as(ref: RefType): this.type = {
		refs += ref;
		ref.bind(this.asInstanceOf[ref.BoundType]);
		this;
	}

	def >(ports: String): ArrowType = {
		val ps = ports.split(":");
		createArrow(ps(0) -> ps(1));
	}
}

case class Relationship(target: BoundNode[_], ports: (String, String)) {
}

class SourceNode(source: BatchSource) extends BoundNode[BatchSource](source) {
	type ArrowType = SourceArrow;
	type RefType = SourceRef;

	override def createProcessor(): Processor = DoLoad(source);

	override def createArrow(ports: (String, String)): SourceArrow =
		new SourceArrow(this, ports);
}

//do not use case class, avoiding BoundSink(MemorySink()) equals BoundSink(MemorySink())
class SinkNode(sink: Sink) extends BoundNode[Sink](sink) {
	type ArrowType = SinkArrow;
	type RefType = SinkRef;
	var _outputMode = OutputMode.Complete();

	def setOutputMode(outputMode: OutputMode) = _outputMode = outputMode;

	override def createProcessor(): Processor = DoWrite(sink, _outputMode);

	override def createArrow(ports: (String, String)): SinkArrow =
		new SinkArrow(this, ports);
}

class ProcessorNode(processor: Processor) extends BoundNode[Processor](processor) {
	type ArrowType = ProcessorArrow;
	type RefType = ProcessorRef;

	override def createProcessor(): Processor = processor;

	override def createArrow(ports: (String, String)): ProcessorArrow =
		new ProcessorArrow(this, ports);
}