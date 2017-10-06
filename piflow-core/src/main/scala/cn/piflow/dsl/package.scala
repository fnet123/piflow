package cn.piflow

import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor

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

  implicit def named(ref: SinkRef): NamedSink = ref.node.named.asInstanceOf[NamedSink];
  implicit def named(ref: SourceRef): NamedSource = ref.node.named.asInstanceOf[NamedSource];
  implicit def named(ref: ProcessorRef): NamedProcessor = ref.node.named.asInstanceOf[NamedProcessor];

  implicit def named(node: PipedProcessorNode): Named[_] = node.named;

  implicit def node2Graph(node: PipedProcessorNode): FlowGraph = node.flowGraph;
}