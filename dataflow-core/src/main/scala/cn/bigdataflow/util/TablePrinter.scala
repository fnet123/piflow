package cn.bigdataflow.util

import org.apache.commons.lang.StringUtils

object TablePrinter {
	def print(columns: Seq[String], data: Seq[Seq[Any]], nullString: String = "(null)") = {
		val formatedColumns: Seq[String] = columns.map(x ⇒
			if (x == null) { nullString } else { x });
		val formatedData: Seq[Seq[String]] = data.map(_.map(
			x ⇒ if (x == null) { nullString } else { x.toString }));

		val sb = new StringBuilder
		val numCols = formatedColumns.length

		// Initialise the width of each column to a minimum value of '3'
		val colWidths = Array.fill(numCols)(3)

		// Compute the width of each column
		for (row ← formatedData :+ formatedColumns) {
			for ((cell, i) ← row.zipWithIndex) {
				colWidths(i) = math.max(colWidths(i), cell.length)
			}
		}

		// Create SeparateLine
		val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

		// column names
		formatedColumns.zipWithIndex.map {
			case (cell, i) ⇒
				StringUtils.rightPad(cell, colWidths(i))
		}.addString(sb, "|", "|", "|\n")

		sb.append(sep)

		// data
		formatedData.map {
			_.zipWithIndex.map {
				case (cell, i) ⇒
					StringUtils.rightPad(cell, colWidths(i))
			}.addString(sb, "|", "|", "|\n")
		}

		sb.append(sep);
		println(sb);
	}
}