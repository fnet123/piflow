package cn.piflow.runner

import cn.piflow.FlowGraphEvent

trait ProcessorEvent {
}

case class ProcessedRows(lines: Option[Long], bytes: Option[Long]) extends ProcessorEvent {
}

case class ProcessorReady() extends ProcessorEvent {
}

case class ProcessorStarted() extends ProcessorEvent {
}

case class ProcessorCompleted() extends ProcessorEvent {

}

case class ProcessorFailed() extends ProcessorEvent {

}

case class EventFromProcessor(nodeId: Int, processorEvent: ProcessorEvent) extends FlowGraphEvent {

}