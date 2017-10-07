package cn.piflow

import cn.piflow.io.{BatchSource, Sink}
import cn.piflow.processor.Processor

package object dsl {
  implicit def withPorts(value: Processor): ProcessorArrow =
    withPorts(bound(value));

  implicit def withPorts(bound: BoundProcessor): ProcessorArrow =
    new ProcessorArrow(bound);

  implicit def withPorts(value: Sink): SinkArrow =
    withPorts(bound(value));

  //convert source/sink/processor to Named first
  implicit def bound(value: Sink): BoundSink =
    new BoundSink(value);

  implicit def withPorts(bound: BoundSink): SinkArrow =
    new SinkArrow(bound);

  implicit def withPorts(value: BatchSource): SourceArrow =
    withPorts(bound(value));

  implicit def bound(value: BatchSource): BoundSource =
    new BoundSource(value);

  implicit def withPorts(bound: BoundSource): SourceArrow =
    new SourceArrow(bound);

  implicit def chained2Graph(chain: ChainWithTail[_]): FlowGraph = {
    val flowGraph = new FlowGraph();
    chain.bindFlowGraph(flowGraph);
    flowGraph;
  }

  implicit def ref2Node(ref: SinkRef): BoundSink = ref.bound;

  implicit def ref2Node(ref: SourceRef): BoundSource = ref.bound;

  implicit def ref2Node(ref: ProcessorRef): BoundProcessor = ref.bound;

  implicit def piped[X](nodes: Seq[Arrow[X]]) =
    new ArrowSeq[X](nodes);

  implicit def chain2Bound(chain: ChainWithProcessorAsTail): BoundProcessor =
    chain.successor.bound.asInstanceOf[BoundProcessor];

  implicit def chain2Bound(chain: ChainWithSourceAsTail): BoundSource =
    chain.successor.bound.asInstanceOf[BoundSource];

  implicit def chain2Bound(chain: ChainWithSinkAsTail): BoundSink =
    chain.successor.bound.asInstanceOf[BoundSink];

  //single node graph
  implicit def asGraph(value: Processor): FlowGraph = {
    asGraph(bound(value));
  }

  implicit def bound(value: Processor): BoundProcessor =
    new BoundProcessor(value);

  implicit def asGraph(bound: BoundProcessor): FlowGraph = {
    val flowGraph = new FlowGraph();
    val node = flowGraph.createNode(bound.createProcessor());
    bound.notifyRefs(node);
    flowGraph;
  }
}