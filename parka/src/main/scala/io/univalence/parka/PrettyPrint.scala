package io.univalence.parka

object PrettyPrint {

  // A améliorer ou détruire
  def prettyPrintHistogram(histogram: Histogram): String = {
    val maxLengthHisto = 6
    val maxLengthHistoBy2 = maxLengthHisto / 2

    val ranges = 0.0 to 1.0 by 0.25
    val qb = ranges.map(histogram.quantileBounds).map(_._1)
    val maxQb = qb.max
    val minQb = qb.min

    val qbProportional = if (maxQb != 0 && minQb != 0) {
      val globalMax = Set(maxQb, -minQb).max
      qb.map(v => ((v * maxLengthHistoBy2) / globalMax).toInt)
    } else if (maxQb != 0) {
      qb.map(v => ((v * maxLengthHisto) / maxQb).toInt)
    } else if (minQb != 0) {
      qb.map(v => - ((v * maxLengthHisto) / minQb).toInt )
    } else qb.map(_.toInt)

    val yAxis = if (maxQb != 0 && minQb != 0) {
      maxLengthHistoBy2 to -maxLengthHistoBy2 by -1
    } else if (maxQb != 0) {
      maxLengthHisto to 0 by -1
    } else 0 to -maxLengthHisto by -1

    val yAxisMax = yAxis.max
    val yAxisMin = yAxis.min

    yAxis.map(y => {
      val value = qbProportional.map(x => {
        if (x >= y && x >= 0 && y >= 0)      "|###|"
        else if (x <= y && x <= 0 && y <= 0) "|###|"
        else if (y != yAxisMin)              "     "
        else                                 "_____"
      }).mkString(" ")
      val yValue = y match {
        case `yAxisMax` => s"$maxQb"
        case `yAxisMin` => s"$minQb"
        case 0 => s"0"
        case _ => ""
      }
      s"|$value  $yValue"
    }).mkString("\n")
  }

  implicit class PrettyPrintUtils(pr: ParkaResult) {
    import sext._

    //def prettyPrint: String = pr.valueTreeString

    def prettyPrint: String =
      s"""inner:
         |  countRowEqual: ${pr.inner.countRowEqual}
         |  countRowNotEqual: ${pr.inner.countRowNotEqual}
         |  countDiffByRow:
         |  |  ${this.prettyPrintDiffByRow(pr.inner.countDiffByRow)}
         |  byColumn:
         outer:
         |  countRow:
         |  |   left: ${pr.outer.countRow.left}
         |  |   right: ${pr.outer.countRow.right}
         |  byColumn:
       """.stripMargin

    def prettyPrintDiffByRow(differences: Map[Seq[String], Long]): String =
      differences.filter(_._1.nonEmpty).map { case (key, value) if key.nonEmpty => "Key(s): (" + key.mkString(",").toString + ") has " + value + " occurrence(s)"}.mkString("\n")
  }
}
