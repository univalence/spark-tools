package io.univalence.parka

import io.univalence.parka.Delta.{ DeltaBoolean, DeltaDate, DeltaDouble, DeltaLong, DeltaString, DeltaTimestamp }

object ParkaPrinter {
  val sep   = "    "
  val start = ""

  def printAccumulator(level: Int, acc: String = "")(): String =
    level match {
      case l if l <= 0 => start + acc + " "
      case _           => printAccumulator(level - 1, acc + sep)
    }

  def printInformation(information: String, field: String, level: Int, jump: Boolean = false): String =
    printAccumulator(level) + s"""$field:${if (jump) "\n" else " "}$information"""

  def printParkaResult(parkaResult: ParkaResult, level: Int = 0): String =
    s"""|${printAccumulator(level)}Parka Result:
        |${printInner(parkaResult.inner, level + 1)}
        |${printOuter(parkaResult.outer, level + 1)}""".stripMargin

  def printInner(inner: Inner, level: Int = 0): String =
    s"""|${printAccumulator(level)}Inner:
        |${printInformation(inner.countRowEqual.toString, "Number of equal row", level + 1)}
        |${printInformation(inner.countRowNotEqual.toString, "Number of different row", level + 1)}
        |${printDiffByRow(inner.countDiffByRow, level + 1)}
        |${printInnerByColumn(inner.byColumn, level + 1)}""".stripMargin

  def printOuter(outer: Outer, level: Int = 0): String =
    s"""|${printAccumulator(level)}Outer:
        |${printInformation(outer.countRow.left.toString, "Number of unique row on the left dataset", level + 1)}
        |${printInformation(outer.countRow.right.toString, "Number of unique row on the right dataset", level + 1)}
        |${printOuterByColumn(outer.byColumn, level + 1)}""".stripMargin

  def printDiffByRow(differences: Map[Seq[String], Long], level: Int = 0): String = {
    val stringifyDiff = differences
      .filter(_._1.nonEmpty)
      .map {
        case (key, value) =>
          printAccumulator(level + 1) + "Key (" + key.mkString(",").toString + ") has " + value + " occurrence" + {
            if (value > 1) "s" else ""
          }
      }
      .mkString("\n")

    printInformation(stringifyDiff, "Differences by sequence of keys", level, true)

  }

  def printInnerByColumn(byColumn: Map[String, Delta], level: Int = 0): String = {
    val stringifyDiff = byColumn.map {
      case (key, value) =>
        s"""|${printAccumulator(level + 1)}$key:
            |${printDelta(value, level + 2)}""".stripMargin
    }.mkString("\n")

    printInformation(stringifyDiff, "Delta by key", level, true)
  }

  def printOuterByColumn(byColumn: Map[String, Both[DescribeV2]], level: Int = 0): String = {
    val stringifyDiff = byColumn.map {
      case (key, value) =>
        s"""|${printAccumulator(level + 1)}$key:
            |${printBothDescribe(value, level + 2)}""".stripMargin
    }.mkString("\n")

    printInformation(stringifyDiff, "Describe by key", level, true)
  }

  def printDelta(delta: Delta, level: Int = 0): String =
    s"""|${printInformation(delta.nEqual.toString, "Number of similarities", level + 1)}
        |${printInformation(delta.nNotEqual.toString, "Number of differences", level + 1)}
        |${printBothDescribe(delta.describe, level + 1)}
        |${printDeltaSpecific(delta, level + 1)}""".stripMargin

  def printDeltaSpecific(delta: Delta, level: Int = 0): String = delta match {
    case deltaLong: DeltaLong     => printHistogram(deltaLong.error, level + 1, "Error's histogram")
    case deltaDouble: DeltaDouble => printHistogram(deltaDouble.error, level + 1, "Error's histogram")
    case deltaString: DeltaString => printHistogram(deltaString.error, level + 1, "Error's histogram")
    case deltaBoolean: DeltaBoolean =>
      s"""|${printInformation(deltaBoolean.ff.toString, "Number of false -> false", level + 1)}
          |${printInformation(deltaBoolean.ft.toString, "Number of false -> true", level + 1)}
          |${printInformation(deltaBoolean.tf.toString, "Number of true -> false", level + 1)}
          |${printInformation(deltaBoolean.tt.toString, "Number of true -> true", level + 1)}""".stripMargin
    case deltaDate: DeltaDate           => printHistogram(deltaDate.error, level + 1, "Error's histogram")
    case deltaTimestamp: DeltaTimestamp => printHistogram(deltaTimestamp.error, level + 1, "Error's histogram")
    case _                              => printInformation("/!\\\\ Can not display this delta", "Error", level + 1)
  }

  def printHistogram(histogram: Histogram, level: Int, name: String = "Histogram"): String = {
    def printDecimal(value: Double): String = f"$value%.2f"
    def fillSpaceBefore(value: String, focus: Int = 0): String = value match {
      case v if v.length >= focus => v
      case v                      => fillSpaceBefore(" " + v, focus)
    }

    val bins    = histogram.bin(6)

    val barMax            = 22
    val maxCount          = bins.map(_.count).max
    val maxLengthBinLower = bins.map(bin => printDecimal(bin.pos).length).max //printDecimal(lastBin.pos).length

    val histobar = "o"

    val stringifyBins = bins
      .map(bin => {
        val binCount = bin.count
        val binRange = fillSpaceBefore(printDecimal(bin.pos), maxLengthBinLower)
        //fillSpaceBefore(printDecimal(bin.pos), maxLengthBinUpper))
        //val binStringify = s"""[${binRange._1}, ${binRange._2}["""
        val binStringify = s"""$binRange"""

        if (binCount > 0) {
          val bar = histobar * (binCount * barMax / maxCount).toInt
          s"${printAccumulator(level)}$binStringify |$histobar$bar $binCount"
        } else {
          s"${printAccumulator(level)}$binStringify |$histobar $binCount"
        }

      })
      .mkString("\n")

    printInformation(stringifyBins, name, level, jump = true)
  }

  def printBoth[A](both: Both[A], level: Int, printA: (A, Int) => String) = {
    def fillSpaceAfter(value: String, focus: Int = 0): String = value match {
      case v if v.length >= focus => v
      case v => fillSpaceAfter(v + " ", focus)
    }

    val left = printInformation(printA(both.left, 1), "Left", 0, true)
    val right = printInformation(printA(both.right, 1), "Right", 0, true)

    val fragmentedLeft = left.split("\n")
    val fragmentedRight = right.split("\n")

    val maxLength = List(fragmentedLeft.map(_.length).max, fragmentedRight.map(_.length).max).max
    val colSize = ConsoleSize.get.columns

    if (colSize < maxLength * 2 + sep.length){
      s"""|${fragmentedLeft.map(printAccumulator(level) + _).mkString("\n")}
          |${fragmentedRight.map(printAccumulator(level) + _).mkString("\n")}t""".stripMargin
    }else{
      val both = fragmentedLeft.zip(fragmentedRight).map{case (l, r) => printAccumulator(level) + fillSpaceAfter(l, maxLength) + printAccumulator(1) + fillSpaceAfter(r, maxLength)}.mkString("\n")
      s"""|$both""".stripMargin
    }
  }

  def printBothDescribe(describes: Both[DescribeV2], level: Int): String = {
    def printOneDescribe(describe: DescribeV2, level: Int): String = describe match {
      /*case DescribeLong(value, _)   => printHistogram(value, level)
      case DescribeDouble(value, _) => printHistogram(value, level)
      case DescribeString(value, _) => printHistogram(value, level)
      case DescribeBoolean(nTrue, nFalse, _) => {
        s"""|${printInformation(nTrue.toString, "Number of false", level)}
            |${printInformation(nFalse.toString, "Number of true", level)}""".stripMargin
      }
      case DescribeDate(period, _) => printHistogram(period, level)
      case DescribeTimestamp(period, _) => printHistogram(period, level)*/
      case DescribeV2(_, m1, m2)  => {
        val histograms = m1.keySet.map(k => printHistogram(m1(k), level)).mkString("\n")
        val counts = m2.keySet.map(k => k match {
            case "nTrue"  => printInformation(m2(k).toString, "Number of true", level)
            case "nFalse" => printInformation(m2(k).toString, "Number of false", level)
            case "nNull"  => printInformation(m2(k).toString, "Number of null", level)
          }).mkString("\n")
        s"$counts\n$histograms"
      }
      case d: DescribeV2 => s"${printAccumulator(level)}Empty describe"
      case _             => printInformation("/!\\ Can not display this describe", "Error", level)
    }

    s"""|${printAccumulator(level)}Describes:
          |${printBoth(describes, level + 1, printOneDescribe)}""".stripMargin
  }

}
