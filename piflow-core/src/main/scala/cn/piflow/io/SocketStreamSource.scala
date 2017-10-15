package cn.piflow.io

import cn.piflow.RunnerContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.TextSocketSource
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
	* Created by bluejoe on 2017/10/10.
	*/
case class SocketStreamSource(host: String, port: Int, includeTimestamp: Boolean = false)
	extends SparkStreamSourceAdapter {

	def createSparkStreamSource( ctx: RunnerContext) =
		new TextSocketSource(host: String, port: Int, includeTimestamp: Boolean, ctx.forType[SQLContext]);
}
