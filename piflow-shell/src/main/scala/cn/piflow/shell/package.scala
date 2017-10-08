package cn.piflow

import cn.piflow.dsl.BoundNode
import cn.piflow.dsl._

package object shell {
	implicit def toRunnable(flowGraph: FlowGraph)(implicit runner: Runner) = {
		new RunnableFlowGraph(flowGraph);
	}

	implicit def toRunnable(node: ChainWithTail[_])(implicit runner: Runner) = {
		new RunnableFlowGraph(asGraph(node));
	}
}