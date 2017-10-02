package cn.piflow

import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor
import cn.piflow.processor.io.DoLoad

package object dsl {
  implicit def piped(nodes: Seq[Named[_]]) =
    new PipedProcessorNodeSeq(nodes);
/*
  implicit def piped(source: BatchSource): PipedProcessorNode = {
    val flowGraph = new FlowGraph();
    val processor = DoLoad(source);
    val processorNode = flowGraph.createNode(processor);
    val ppn = new PipedProcessorNode(flowGraph, processorNode, source, named(source));
    new NamedSource(source).bindProcessorNode(ppn);
    ppn;
  }
*/
  //convert source/sink/processor to Named first
  implicit def named(value: Sink): NamedSink =
    new NamedSink(value);

  implicit def named(value: BatchSource): NamedSource =
    new NamedSource(value);

  implicit def named(value: Processor): NamedProcessor =
    new NamedProcessor(value);

  //TODO
  implicit def named(ref: NodeRef): NamedSink = ref.get.named.asInstanceOf[NamedSink];
  implicit def named(node: PipedProcessorNode): Named[_] = node.named;

  implicit def node2Graph(node: PipedProcessorNode): FlowGraph = node.flowGraph;
}