package io.univalence.parka

import cats.Monoid

//import io.univalence.parka.Delta.{ DeltaBoolean, DeltaDate, DeltaDouble, DeltaLong, DeltaString, DeltaTimestamp }

object ParkaPrinter {
  val sep   = "    "
  val start = ""

  def printAccumulator(level: Int, acc: String = "")(): String =
    level match {
      case l if l <= 0 => start + acc + " "
      case _           => printAccumulator(level - 1, acc + sep)
    }

  import MonoidGen._

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
        |${printOuterByColumn(bmxToMbx(outer.byColumn), level + 1)}""".stripMargin

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

    printInformation(stringifyDiff, "Differences by sequence of keys", level, jump = true)

  }

  def printInnerByColumn(byColumn: Map[String, Delta], level: Int = 0): String = {
    val stringifyDiff = byColumn.map {
      case (key, value) =>
        s"""|${printAccumulator(level + 1)}$key:
            |${printDelta(value, level + 2)}""".stripMargin
    }.mkString("\n")

    printInformation(stringifyDiff, "Delta by key", level, jump = true)
  }

  def bmxToMbx[K, T: Monoid](bmx: Both[Map[K, T]]): Map[K, Both[T]] = {
    val mono = implicitly[Monoid[T]]

    import MonoidGen._
    implicitly[Monoid[Map[K, Both[T]]]]
      .combine(bmx.left.mapValues(x => Both(x, mono.empty)), bmx.right.mapValues(x => Both(mono.empty, x)))
  }

  def printOuterByColumn(byColumn: Map[String, Both[Describe]], level: Int = 0): String = {
    val stringifyDiff = byColumn.map {
      case (key, value) =>
        s"""|${printAccumulator(level + 1)}$key:
            |${printBothDescribe(value, level + 2)}""".stripMargin
    }.mkString("\n")

    printInformation(stringifyDiff, "Describe by key", level, jump = true)
  }

  def printDelta(delta: Delta, level: Int = 0): String =
    s"""|${printInformation(delta.nEqual.toString, "Number of similarities", level + 1)}
        |${printInformation(delta.nNotEqual.toString, "Number of differences", level + 1)}
        |${printBothDescribe(delta.describe, level + 1)}
        |${printDeltaSpecific(delta, level + 1)}""".stripMargin

  def printDeltaSpecific(delta: Delta, level: Int = 0): String = delta match {
    case Delta(nEqual, nNotEqual, describe, error) =>
      val m1         = error.histograms
      val m2         = error.counts
      val histograms = error.histograms.keySet.map(k => printHistogram(m1(k), level)).mkString("\n")
      val counts = error.counts.keySet.map {
        case k @ "tf"          => printInformation(m2(k).toString, "# true -> false", level)
        case k @ "ft"          => printInformation(m2(k).toString, "# false -> true", level)
        case k @ "rightToNull" => printInformation(m2(k).toString, "# null -> not null", level)
        case k @ "leftToNull"  => printInformation(m2(k).toString, "# not null -> null", level)
      }.mkString("\n")
      s"$counts\n$histograms"
  }

  def printHistogram(histogram: Histogram, level: Int, name: String = "Histogram"): String = {
    def printDecimal(value: Double): String = f"$value%.2f"
    def fillSpaceBefore(value: String, focus: Int = 0): String = value match {
      case v if v.length >= focus => v
      case v                      => fillSpaceBefore(" " + v, focus)
    }

    val bins = histogram.bin(6)

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

  def printBoth[A](both: Both[A], level: Int, printA: (A, Int) => String): String = {
    def fillSpaceAfter(value: String, focus: Int = 0): String = value match {
      case v if v.length >= focus => v
      case v                      => fillSpaceAfter(v + " ", focus)
    }

    val left  = printInformation(printA(both.left, 1), "Left", 0, jump   = true)
    val right = printInformation(printA(both.right, 1), "Right", 0, jump = true)

    val fragmentedLeft  = left.split("\n")
    val fragmentedRight = right.split("\n")

    val maxLengthLeft  = fragmentedLeft.map(_.length).max
    val maxLengthRight = fragmentedRight.map(_.length).max

    val colSize = ConsoleSize.get.columns

    if (colSize < maxLengthLeft + maxLengthRight + sep.length) {
      s"""|${fragmentedLeft.map(printAccumulator(level) + _).mkString("\n")}
          |${fragmentedRight.map(printAccumulator(level) + _).mkString("\n")}t""".stripMargin
    } else {
      val both = fragmentedLeft
        .zipAll(fragmentedRight, "", "")
        .map {
          case (l, r) =>
            printAccumulator(level) + fillSpaceAfter(l, maxLengthLeft) + printAccumulator(1) + fillSpaceAfter(
              r,
              maxLengthRight
            )
        }
        .mkString("\n")
      s"""|$both""".stripMargin
    }
  }

  def printOneDescribe(describe: Describe, level: Int): String = {
    import describe._
    val strHistogram: String = histograms.keySet.map(k => printHistogram(histograms(k), level)).mkString("\n")
    val strCounts = counts.keySet.map {
      case k @ "nTrue"  => printInformation(counts(k).toString, "Number of true", level)
      case k @ "nFalse" => printInformation(counts(k).toString, "Number of false", level)
      case k @ "nNull"  => printInformation(counts(k).toString, "Number of null", level)
    }.mkString("\n")

    if (strHistogram.isEmpty) s"$strCounts"
    else if (strCounts.isEmpty) s"$strHistogram"
    else s"$strCounts\n$strHistogram"

  }

  def printBothDescribe(describes: Both[Describe], level: Int): String =
    s"""|${printAccumulator(level)}Describes:
        |${printBoth(describes, level + 1, printOneDescribe)}""".stripMargin

}
