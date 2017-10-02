package cn.piflow.dsl

import cn.piflow.FlowGraph
import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2017/10/1.
  */

//Named wraps a Processor, enables naming, assigning port names
//SeqAsSource(...) % sourceRef / "_1:_1"

abstract class Named[T](val value: T) {
  var ports = "_1" -> "_1";
  val nodes = ArrayBuffer[NodeRef]();

  def %(ref: NodeRef): this.type = as(ref);

  def as(ref: NodeRef): this.type = {
    nodes += ref;
    this;
  }

  def %(ref: Ref): this.type = as(ref);

  def as(ref: Ref): this.type = {
    ref() = value;
    this;
  }

  def /(ports: String): this.type = {
    val ps = ports.split(":");
    this.ports = ps(0) -> ps(1);
    this;
  }

  def getProcessor(): Processor;

  def bindProcessorNode(node: PipedProcessorNode) = {
    nodes.foreach(_.update(node));
  }

  def getGraph() = new FlowGraph();

  //TODO: extract a trait
  //TODO: Append or Complete
  def >>(sink: NamedSink): PipedProcessorNode = {
    pipeNext(sink);
  }

  def >(sink: NamedSink): PipedProcessorNode = {
    pipeNext(sink);
  }

  //DoWrite(sink.value, OutputMode.Complete), sink.value

  def >(processor: NamedProcessor): PipedProcessorNode = {
    pipeNext(processor);
  }

  /*
  def >(successorNode: PipedProcessorNode): PipedProcessorNode = {
    flowGraph.link(processorNode, successorNode.processorNode, ports);
    successorNode;
  }
  */


  def pipeNext(successor: Named[_]): PipedProcessorNode = {
    val flowGraph = getGraph();
    val processorNode = flowGraph.createNode(getProcessor());
    val successorNode = flowGraph.createNode(successor.getProcessor());
    flowGraph.link(processorNode, successorNode, ports);
    val ppn1 = new PipedProcessorNode(flowGraph, processorNode, value, this);
    this.bindProcessorNode(ppn1);
    val ppn2 = new PipedProcessorNode(flowGraph, successorNode, successor.value, successor);
    successor.bindProcessorNode(ppn2);
    ppn2;
  }
}

class NamedSink(value: Sink) extends Named[Sink](value) {
  def getProcessor(): Processor = null;
}

class NamedSource(value: BatchSource) extends Named[BatchSource](value) {
  def getProcessor(): Processor = null;
}

class NamedProcessor(value: Processor) extends Named[Processor](value) {
  def getProcessor(): Processor = null;
}
