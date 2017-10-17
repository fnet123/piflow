package cn.piflow

import java.util.Date
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

/**
	* Created by bluejoe on 2017/10/16.
	*/
object ProcessorStatus {
	val READY = ProcessorStatus("READY");
	val RUNNING = ProcessorStatus("RUNNING");
	val COMPLETED = ProcessorStatus("COMPLETED");
	val FAILED = ProcessorStatus("FAILED");
}

case class ProcessorStatus(lable: String) {
	override def toString = lable;
}

trait FlowJobStat {
	def nodes(): Map[Int, FlowNodeStat];

	def show();
}

trait FlowNodeStat {
	def rows: Option[Long];

	def bytes: Option[Long];

	def startTime: Option[Date];

	def endTime: Option[Date];

	def status: Option[ProcessorStatus];

	def rps: Option[Long];

	def bps: Option[Long];
}