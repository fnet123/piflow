package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.TextSocketSource
import org.apache.spark.sql.streaming.OutputMode

/**
	* Created by bluejoe on 2017/10/10.
	*/
case class SocketStreamSource(host: String, port: Int, includeTimestamp: Boolean = false) extends SparkStreamSourceAdapter {
	def createSparkStreamSource(outputMode: OutputMode, ctx: RunnerContext) =
		new TextSocketSource(host: String, port: Int, includeTimestamp: Boolean, ctx.forType[SQLContext]);
}
