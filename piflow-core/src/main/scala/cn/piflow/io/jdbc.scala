package cn.piflow.io

import cn.piflow.JobContext
import org.apache.spark.sql.{SQLContext, Dataset}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.streaming.OutputMode

/**
	* Created by bluejoe on 2017/10/9.
	*/
class JdbcTableSink(parameters: Map[String, String]) extends BatchSink {
	override def destroy(): Unit = {

	}

	var _sqlContext: SQLContext = null;

	var _outputMode: OutputMode = null;

	override def init(outputMode: OutputMode, ctx: JobContext) = {
		_sqlContext = ctx.sqlContext();
		_outputMode = outputMode;
	}

	def writeBatch(ds: Dataset[_]): Unit = {
		val sparkRelationProvider = new JdbcRelationProvider();
		sparkRelationProvider.createRelation(_sqlContext,
			SparkIOSupport.outputMode2SaveMode(_outputMode),
			parameters,
			ds.toDF()
		);
	}
}
