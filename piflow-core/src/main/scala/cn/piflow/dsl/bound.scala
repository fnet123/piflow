package cn.piflow.dsl

import cn.piflow.ProcessorNode
import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor
import cn.piflow.processor.io.{DoLoad, DoWrite}
import org.apache.spark.sql.streaming.OutputMode

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2017/10/1.
  */
case class Sibling(target: BoundNode[_], ports: (String, String)) {
}

abstract class BoundNode[T](val userData: T) {
  type NodeWithPortType <: Arrow[T];
  type RefType <: Ref[T];
  val successors = ArrayBuffer[Sibling]();
  val predecessors = ArrayBuffer[Sibling]();
  private val refs = ArrayBuffer[RefType]();

  def createProcessor(): Processor;

  def notifyRefs(node: ProcessorNode): Unit = {
    refs.foreach(_.bind(node));
  }

  def addSuccessor(target: BoundNode[_], ports: (String, String)): Unit = {
    successors += Sibling(target, ports);
  }

  def addPredecessor(target: BoundNode[_], ports: (String, String)): Unit = {
    predecessors += Sibling(target, ports);
  }

  def createNodeWithPort(ports: (String, String)): NodeWithPortType;

  def %(ref: RefType): this.type = as(ref);

  def as(ref: RefType): this.type = {
    refs += ref;
    ref.bind(this.asInstanceOf[ref.BoundType]);
    this;
  }

  def /(ports: String): NodeWithPortType = {
    val ps = ports.split(":");
    createNodeWithPort(ps(0) -> ps(1));
  }
}

class BoundSource(source: BatchSource) extends BoundNode[BatchSource](source) {
  type NodeWithPortType = SourceArrow;
  type RefType = SourceRef;

  override def createProcessor(): Processor = DoLoad(source);

  override def createNodeWithPort(ports: (String, String)): SourceArrow =
    new SourceArrow(this, ports);
}

//do not use case class, avoiding BoundSink(MemorySink()) equals BoundSink(MemorySink())
class BoundSink(sink: Sink) extends BoundNode[Sink](sink) {
  type NodeWithPortType = SinkArrow;
  type RefType = SinkRef;

  //FIXME
  override def createProcessor(): Processor = DoWrite(sink, OutputMode.Complete);

  override def createNodeWithPort(ports: (String, String)): SinkArrow =
    new SinkArrow(this, ports);
}

class BoundProcessor(processor: Processor) extends BoundNode[Processor](processor) {
  type NodeWithPortType = ProcessorArrow;
  type RefType = ProcessorRef;

  override def createProcessor(): Processor = processor;

  override def createNodeWithPort(ports: (String, String)): ProcessorArrow =
    new ProcessorArrow(this, ports);
}