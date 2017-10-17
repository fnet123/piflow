package cn.piflow.processor.io

import java.util.concurrent.atomic.AtomicInteger

import cn.piflow.{ProcessorContext, JobContext}
import cn.piflow.io._
import cn.piflow.processor.{Processor021, Processor120}
import cn.piflow.util.ReflectUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.{Sink => SparkStreamSink, Source => SparkStreamSource, _}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.SystemClock

/**
	* @author bluejoe2008@gmail.com
	*/
case class DoLoad(source: Source)
	extends Processor021 {
	override def perform021(): Dataset[_] = {
		source match {
			case bs: BatchSource => loadBatch(bs);
			case ss: StreamSource => loadStream(ss);
		}
	}

	def loadBatch(bs: BatchSource): Dataset[_] = {
		bs.init(_processorContext);
		bs.loadBatch();
	}

	//TODO: bad code here
	def loadStream(ss: StreamSource): Dataset[_] = {
		ss.init(_processorContext);
		// Dataset.ofRows(sparkSession, StreamingRelation(dataSource))
		val sparkSession = _processorContext.sparkSession;

		class SparkDataSource extends DataSource(sparkSession, "NeverUsedProviderClassName") {
			override def createSource(metadataPath: String) = asSparkStreamSource(ss);
			override lazy val sourceInfo = SourceInfo(Util.getNextStreamSourceName(), ss.schema, Nil)
		}

		val dataSource = new SparkDataSource();
		val sr = StreamingRelation(dataSource);
		singleton[Dataset[Row]]._call("ofRows")(sparkSession, sr).asInstanceOf[Dataset[_]];
	}

	private def asSparkStreamSource(ss: StreamSource) = new SparkStreamSource() {
		override def schema: StructType = null;


		override def getOffset: Option[Offset] = {
			SparkIOSupport.toOffsetOption(ss.getOffset);
		}

		override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
			ss.loadBatch(SparkIOSupport.valueOf(start), SparkIOSupport.valueOf(end)).toDF();
		}

		override def commit(end: Offset): Unit = {}

		override def stop(): Unit = {}
	}
}

private object Util {
	val queryId = new AtomicInteger(0);
	val sourceId = new AtomicInteger(0);

	def getNextStreamSourceName() = "stream-source-" + sourceId.incrementAndGet();

	//TODO: actually never used
	def getNextQueryId() = "query-" + queryId.incrementAndGet();
}
