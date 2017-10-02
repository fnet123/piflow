package cn.piflow.dsl

import cn.piflow.processor.Processor
import cn.piflow.processor.io.DoWrite
import cn.piflow.{FlowException, FlowGraph, ProcessorNode}
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by bluejoe on 2017/10/1.
  */
class PipedProcessorNode(val flowGraph: FlowGraph,
                         val processorNode: ProcessorNode,
                         val bindingValue: Any,
                         val named: Named[_])
{
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
    val successorNode = flowGraph.createNode(successor.getProcessor());
    flowGraph.link(processorNode, successorNode, named.ports);
     val ppn2 = new PipedProcessorNode(flowGraph, successorNode, successor.value, successor);
    successor.bindProcessorNode(ppn2);
    ppn2;
  }

  implicit def ref2Node(successorNode: NodeRef): PipedProcessorNode =
    successorNode.get;
}

class PipedProcessorNodeSeq(nodes: Seq[Named[_]]) {
  def >(processor: NamedProcessor): PipedProcessorNode = {
    if (nodes.length >= 1) {
      val successorNode = nodes(0) > processor;
      nodes.drop(1).foreach(_.pipeNext(successorNode.named));
      successorNode;
    }
    else {
      throw new FlowException("wrong number of nodes: empty");
    }
  }
}
