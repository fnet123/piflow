package cn.piflow.runner

import java.util.Date

import cn.piflow._
import cn.piflow.util.FormatUtils

class FlowNodeStatImpl() extends FlowNodeStat {
	var _rows: Option[Long] = None
	var _bytes: Option[Long] = None
	var _status: Option[ProcessorStatus] = None
	var _startTime: Option[Date] = None
	var _endTime: Option[Date] = None
	var _timeCost: Option[Long] = None

	def rows: Option[Long] = _rows;

	def bytes: Option[Long] = _bytes;

	def startTime: Option[Date] = _startTime;

	def endTime: Option[Date] = _endTime;

	def status: Option[ProcessorStatus] = _status;

	def processorCompleted() = {
		_status = Some(ProcessorStatus.COMPLETED);
		_endTime = Some(new Date());
		if (_startTime.isDefined)
			_timeCost = Some(_endTime.get.getTime - _startTime.get.getTime)
	}

	def processorFailed() = {
		_status = Some(ProcessorStatus.FAILED);
		_endTime = Some(new Date());
	}

	def processorStarted() = {
		_status = Some(ProcessorStatus.RUNNING);
		_startTime = Some(new Date());
	}

	def processorReady() = {
		_status = Some(ProcessorStatus.READY);
	}

	def addRows(lines: Option[Long], bytes: Option[Long]) {
		if (lines.isDefined) {
			_rows = Some(_rows.getOrElse(0L) + lines.get);
		}
		if (bytes.isDefined) {
			_bytes = Some(_bytes.getOrElse(0L) + bytes.get);
		}
	}

	def rps: Option[Long] = {
		if (_rows.isDefined && _startTime.isDefined) {
			val cost = _timeCost.getOrElse(System.currentTimeMillis() - _startTime.get.getTime);
			if (cost == 0)
				None
			else
				Some(_rows.get * 1000L / cost)
		}
		else
			None
	}

	def bps: Option[Long] = {
		if (_bytes.isDefined && _startTime.isDefined) {
			val cost = _timeCost.getOrElse(System.currentTimeMillis() - _startTime.get.getTime);
			if (cost == 0)
				None
			else
				Some(_bytes.get * 1000L / cost)
		}
		else
			None
	}
}

class FlowJobStatImpl(jobInstanceId: String) extends FlowJobStat with Logging {
	val stats = collection.mutable.Map[Int, FlowNodeStat]();

	def show() = {
		val data = stats
			.map { pair ⇒
				val nodeId = pair._1;
				val nodeStat = pair._2;
				Seq[Any](nodeId, nodeStat.status, nodeStat.startTime, nodeStat.endTime,
					nodeStat.rows, nodeStat.bytes, nodeStat.rps, nodeStat.bps);
			}.toSeq.sortBy(_.apply(0).asInstanceOf[Int])

		FormatUtils.printTable(Seq("node", "status", "start time", "end time", "rows",
			"bytes", "rows/s", "bytes/s"), data, "");
	}

	def getNodeStat(nodeId: Int) = {
		stats.getOrElseUpdate(nodeId, new FlowNodeStatImpl()).asInstanceOf[FlowNodeStatImpl];
	}

	def receive(event: FlowGraphEvent): Unit = {
		logger.debug(s"job $jobInstanceId received event: $event");

		event match {
			case EventFromProcessor(nodeId: Int, ProcessedRows(lines: Option[Long], bytes: Option[Long]))
			=> getNodeStat(nodeId).addRows(lines, bytes);
			case EventFromProcessor(nodeId: Int, ProcessorReady())
			=> getNodeStat(nodeId).processorReady();
			case EventFromProcessor(nodeId: Int, ProcessorStarted())
			=> getNodeStat(nodeId).processorStarted();
			case EventFromProcessor(nodeId: Int, ProcessorFailed())
			=> getNodeStat(nodeId).processorFailed();
			case EventFromProcessor(nodeId: Int, ProcessorCompleted())
			=> getNodeStat(nodeId).processorCompleted();
		}
	}

	def nodes() = stats.toMap;
}

