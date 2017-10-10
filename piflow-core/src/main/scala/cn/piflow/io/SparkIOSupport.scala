package cn.piflow.io

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset}
import org.apache.spark.sql.streaming.OutputMode

/**
	* Created by bluejoe on 2017/10/10.
	*/
object SparkIOSupport {
	def outputMode2SaveMode(outputMode: OutputMode): SaveMode = {
		if (outputMode == OutputMode.Complete())
			SaveMode.Overwrite;
		else if (outputMode == OutputMode.Append())
			SaveMode.Append;
		else
			SaveMode.Ignore;
	}

	def valueOf(offset: Option[Offset]): Long = {
		LongOffset.convert(offset.getOrElse(LongOffset(-1))).getOrElse(LongOffset(-1)).offset;
	}

	def valueOf(offset: Offset): Long = {
		LongOffset.convert(offset).getOrElse(LongOffset(-1)).offset;
	}

	def toOffset(offset: Long) = {
		LongOffset(offset)
	}

	def toOffsetOption(offset: Long): Option[Offset] = {
		if (offset < 0)
			None;
		else
			Some(LongOffset(offset));
	}
}
