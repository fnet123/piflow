package cn.piflow

import cn.piflow.io.{BatchSource, Sink}

package object dsl {
	//convert source/sink/processor to Named first
	implicit def asNode(value: Sink): SinkNode =
		new SinkNode(value);

	implicit def asNode(value: BatchSource): SourceNode =
		new SourceNode(value);

	implicit def asNode(value: Processor): ProcessorNode =
		new ProcessorNode(value);

	implicit def asNode(ref: SinkRef): SinkNode = ref.bound;

	implicit def asNode(ref: SourceRef): SourceNode = ref.bound;

	implicit def asNode(ref: ProcessorRef): ProcessorNode = ref.bound;

	implicit def asArrow[X](nodes: Seq[Arrow[X]]) =
		new ArrowSeq[X](nodes);

	implicit def asNode(chain: ChainWithProcessorAsTail): ProcessorNode =
		chain.successor.node.asInstanceOf[ProcessorNode];

	implicit def asNode(chain: ChainWithSourceAsTail): SourceNode =
		chain.successor.node.asInstanceOf[SourceNode];

	implicit def asNode(chain: ChainWithSinkAsTail): SinkNode =
		chain.successor.node.asInstanceOf[SinkNode];

	//single node graph
	implicit def asGraph(chain: ChainWithTail[_]): FlowGraph = {
		val flowGraph = new FlowGraph();
		chain.bindFlowGraph(flowGraph);
		flowGraph;
	}

	implicit def asGraph(value: Processor): FlowGraph = {
		asGraph(asNode(value));
	}

	implicit def asGraph(bound: ProcessorNode): FlowGraph = {
		val flowGraph = new FlowGraph();
		val node = flowGraph.createNode(bound.createProcessor());
		bound.notifyRefs(node);
		flowGraph;
	}
}