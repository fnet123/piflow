package cn.bigdataflow;

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import scala.reflect.ClassTag

object TablePrinter {
	def print(columns: Seq[String], data: Seq[Seq[Any]]) = {
		val sb = new StringBuilder
		val numCols = columns.length

		// Initialise the width of each column to a minimum value of '3'
		val colWidths = Array.fill(numCols)(3)

		// Compute the width of each column
		for (row ← data :+ columns) {
			for ((cell, i) ← row.zipWithIndex) {
				colWidths(i) = math.max(colWidths(i), cell.toString().length)
			}
		}

		// Create SeparateLine
		val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

		// column names
		columns.zipWithIndex.map {
			case (cell, i) ⇒
				StringUtils.rightPad(cell, colWidths(i))
		}.addString(sb, "|", "|", "|\n")

		sb.append(sep)

		// data
		data.map {
			_.zipWithIndex.map {
				case (cell, i) ⇒
					StringUtils.rightPad(cell.toString, colWidths(i))
			}.addString(sb, "|", "|", "|\n")
		}

		sb.append(sep);
		println(sb);
	}
}