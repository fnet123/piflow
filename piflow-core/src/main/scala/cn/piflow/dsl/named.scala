package cn.piflow.dsl

import cn.piflow.FlowGraph
import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor
import cn.piflow.processor.io.{DoLoad, DoWrite}
import org.apache.spark.sql.streaming.OutputMode

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2017/10/1.
  */

//Named wraps a Processor, enables naming, assigning port names
//SeqAsSource(...) % sourceRef / "_1:_1"

abstract class Named[T](val value: AnyRef) {
  var ports = "_1" -> "_1";
  val nodes = ArrayBuffer[Ref]();

  def %(ref: Ref): this.type = as(ref);

  def as(ref: Ref): this.type = {
    nodes += ref;
    ref.bindValue(this.value);
    this;
  }

  def /(ports: String): this.type = {
    val ps = ports.split(":");
    this.ports = ps(0) -> ps(1);
    this;
  }

  def createProcessor(append: Boolean): Processor;

  def bindProcessorNode(node: PipedProcessorNode) = {
    nodes.foreach(_.bindNode(node));
  }

  //TODO: extract a trait Piped
  def >>(sink: NamedSink): PipedProcessorNode = {
    pipeNext(sink, true);
  }

  def >(sink: NamedSink): PipedProcessorNode = {
    pipeNext(sink, false);
  }

  //DoWrite(sink.value, OutputMode.Complete), sink.value

  def >(processor: NamedProcessor): PipedProcessorNode = {
    pipeNext(processor, false);
  }

  /*
  def >(successorNode: PipedProcessorNode): PipedProcessorNode = {
    flowGraph.link(processorNode, successorNode.processorNode, ports);
    successorNode;
  }
  */

def getOrCreateNode(append: Boolean):PipedProcessorNode={
  val flowGraph = new FlowGraph();
  val processorNode = flowGraph.createNode(createProcessor(append));
  new PipedProcessorNode(flowGraph, processorNode, value, this);
}

  def pipeNext(successor: Named[_], append: Boolean): PipedProcessorNode = {
    val ppn = getOrCreateNode(append);
    val flowGraph = ppn.flowGraph;
    val processorNode = ppn.processorNode;

    val successorNode = flowGraph.createNode(successor.createProcessor(append));
    flowGraph.link(processorNode, successorNode, ports);

    this.bindProcessorNode(ppn);
    val piped = new PipedProcessorNode(flowGraph, successorNode, successor.value, successor);
    successor.bindProcessorNode(piped);
    piped;
  }
}

class NamedSink(sink: Sink) extends Named[Sink](sink) {
  def createProcessor(append: Boolean): Processor = {
    if (append)
      DoWrite(sink, OutputMode.Append);
    else
      DoWrite(sink, OutputMode.Complete);
  }
}

class NamedSource(source: BatchSource) extends Named[BatchSource](source) {
  def createProcessor(append: Boolean): Processor = DoLoad(source);
}

class NamedProcessor(processor: Processor) extends Named[Processor](processor) {
  def createProcessor(append: Boolean): Processor = processor;
}
