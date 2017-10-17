package cn.piflow.io

import cn.piflow.JobContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.execution.streaming.http.{HttpStreamSource => SparkHttpStreamSource}
import org.apache.spark.sql.execution.streaming.http.{HttpStreamSink => SparkHttpStreamSink}

/**
	* Created by bluejoe on 2017/10/14.
	*/
case class HttpStreamSource(httpServletUrl: String,
                            topic: String,
                            msPollingPeriod: Int = 10,
                            includesTimestamp: Boolean = false,
                            timestampColumnName: String = "_TIMESTAMP_")
	extends SparkStreamSourceAdapter with StreamSource {
	def createSparkStreamSource(ctx: JobContext): SparkHttpStreamSource =
		new SparkHttpStreamSource(ctx.sqlContext(),
			httpServletUrl,
			topic,
			msPollingPeriod,
			includesTimestamp,
			timestampColumnName);
}

case class HttpStreamSink(httpPostURL: String, topic: String, maxPacketSize: Int = 10 * 1024 * 1024)
	extends SparkStreamSinkAdapter with StreamSink {
	def createSparkStreamSink(outputMode: OutputMode, ctx: JobContext) =
		new SparkHttpStreamSink(httpPostURL, topic, maxPacketSize);

}
