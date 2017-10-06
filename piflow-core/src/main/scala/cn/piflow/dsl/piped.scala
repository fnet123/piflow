package cn.piflow.dsl

import cn.piflow.{FlowException, FlowGraph, ProcessorNode}

/**
  * Created by bluejoe on 2017/10/1.
  */
class PipedProcessorNode(val flowGraph: FlowGraph,
                         val processorNode: ProcessorNode,
                         val bindingValue: Any,
                         val named: Named[_]) {
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

  def pipeNext(successor: Named[_], append: Boolean): PipedProcessorNode = {
    val successorNode = flowGraph.createNode(successor.createProcessor(append));
    flowGraph.link(processorNode, successorNode, named.ports);
    val piped = new PipedProcessorNode(flowGraph, successorNode, successor.value, successor);
    successor.bindProcessorNode(piped);
    piped;
  }

  implicit def ref2Node(successorNode: Ref): PipedProcessorNode =
    successorNode.node;
}

class PipedProcessorNodeSeq(nodes: Seq[Named[_]]) {
  def >(processor: NamedProcessor): PipedProcessorNode = {
    pipeNext(processor, false);
  }

  def pipeNext(successor: Named[_], append: Boolean): PipedProcessorNode = {
    if (nodes.length >= 1) {
      val successorNode = nodes(0).pipeNext(successor, append);
      nodes.drop(1).foreach(_.pipeNext(successorNode.named, append));
      successorNode;
    }
    else {
      throw new FlowException("wrong number of nodes: empty");
    }
  }

  def >>(sink: NamedSink): PipedProcessorNode = {
    pipeNext(sink, true);
  }

  def >(sink: NamedSink): PipedProcessorNode = {
    pipeNext(sink, false);
  }

}
