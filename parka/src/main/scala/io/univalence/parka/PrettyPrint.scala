package io.univalence.parka

import io.univalence.parka.Delta.{DeltaBoolean, DeltaDouble, DeltaLong, DeltaString}
import io.univalence.parka.Describe.{DescribeBoolean, DescribeDouble, DescribeLong, DescribeString}

  object ParkaPrinter {
    val sep = "    "
    val start = ""

    def printAccumulator(level: Int, acc: String = "", separator: String = sep)(): String = {
      level match {
        case l if l <= 0 => start + acc + " "
        case _           => printAccumulator(level - 1, acc + separator, separator)
      }
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
            printAccumulator(level + 1) + "Key(s): (" + key.mkString(",").toString + ") has " + value + " occurrence(s)"
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

    def printOuterByColumn(byColumn: Map[String, Both[Describe]], level: Int = 0): String = {
      val stringifyDiff = byColumn.map {
        case (key, value) =>
          s"""|${printAccumulator(level + 1)}$key:
              |${printBothDescribe(value, level + 2)}""".stripMargin
      }.mkString("\n")

      /*
      s"""|${printAccumulator(level)}Describe by key:
          |$stringifyDiff""".stripMargin

       */
      printInformation(stringifyDiff, "Describe by key", level, true)
    }

    def printDelta(delta: Delta, level: Int = 0): String =
      s"""|${printInformation(delta.nEqual.toString, "Number of similarities", level + 1)}
          |${printInformation(delta.nNotEqual.toString, "Number of differences", level + 1)}
          |${printBothDescribe(delta.describe, level + 1)}
          |${printDeltaSpecific(delta, level + 1)}""".stripMargin

    def printDeltaSpecific(delta: Delta, level: Int = 0): String = delta match {
      case deltaLong: DeltaLong   => printHistogram(deltaLong.error, level + 1, "Error's histogram")
      case deltaLong: DeltaDouble => printHistogram(deltaLong.error, level + 1, "Error's histogram")
      case deltaLong: DeltaString => printHistogram(deltaLong.error, level + 1, "Error's histogram")
      case delta: DeltaBoolean =>
        s"""|${printInformation(delta.ff.toString, "Number of false -> false", level + 1)}
            |${printInformation(delta.ft.toString, "Number of false -> true", level + 1)}
            |${printInformation(delta.tf.toString, "Number of true -> false", level + 1)}
            |${printInformation(delta.tt.toString, "Number of true -> true", level + 1)}""".stripMargin
      case _ => printInformation("/!\\\\ Can not display this delta", "Error", level + 1)
    }

    /*def printHistogramHorizontal(histogram: Histogram, level: Int = 0, name: String = "Histogram"): String = {
      def printDecimal(value: Double): String = f"$value%.2f"

      def fillSpaceBefore(value: String, focus: Int = 0): String = value match {
        case v if v.length >= focus => v
        case v => fillSpaceBefore(" " + v, focus)
      }

      def fillSpaceAfter(value: String, focus: Int = 0): String = value match {
        case v if v.length >= focus => v
        case v => fillSpaceAfter(v + " ", focus)
      }

      def fillSpaceBetween(value: String, focus: Int = 0, left: Boolean = true): String = (value, left) match {
        case (v, _) if v.length >= focus => v
        case (v, true) => fillSpaceBetween(" " + v, focus, false)
        case (v, false) => fillSpaceBetween(v + " ", focus, true)
      }

      val height = 20
      val width = 6

      val bins = histogram.bin(width)
      val maxCount = bins.map(_.count).max

      val ordinates = (1 to height).reverse.map(_.toDouble * maxCount / height)

      val maxLengthOrdinate = ordinates.map(_.toString.length).max
      val maxLengthBin = bins.map(b => printDecimal(b.pos).toString.length).max

      val stringifyBar = ordinates.map(
        ordinate =>
          s"""${printAccumulator(level + 1)}${fillSpaceBefore(ordinate.toString, maxLengthOrdinate)} | ${bins.map(_.count).map(count => if (count >= ordinate && count != 0) fillSpaceBetween("o", maxLengthBin) else " " * maxLengthBin).mkString(" ")}"""
      ).mkString("\n")

      val stringyBase =
        s"""|${printAccumulator(level + 1)}${fillSpaceBefore("0", maxLengthOrdinate)} +${"-" * ((maxLengthBin + 1) * width)}
            |${printAccumulator(level + 1)}${" " * (maxLengthOrdinate + 3)}${bins.map(_.pos).map(min => fillSpaceBetween(printDecimal(min), maxLengthBin)).mkString(" ")}""".stripMargin

      val stringifyBins = bins
        .map(bin => {
          val binCount         = bin.count
          val binMin           = printDecimal(bin.pos)
          val binMinWithSpaces = " " * (maxLengthBin - binMin.length) + binMin
          if (binCount > 0) {
            val bar = "█" * (binCount * height / maxCount).toInt
            s"${printAccumulator(level + 1)}$binMinWithSpaces | $bar $binCount"
          } else {
            s"${printAccumulator(level + 1)}$binMinWithSpaces | $binCount"
          }
        })
        .mkString("\n")

      /*
      s"""|${printAccumulator(level)}$name:
          |$stringifyBar
          |$stringyBase""".stripMargin

       */
      printInformation(stringifyBar + "\n" + stringyBase, name, level, true)
    }*/


    def printHistogram(histogram: Histogram, level: Int, name: String = "Histogram"): String = {
      def printDecimal(value: Double): String = f"$value%.2f"
      def fillSpaceBefore(value: String, focus: Int = 0): String = value match {
        case v if v.length >= focus => v
        case v => fillSpaceBefore(" " + v, focus)
      }

      val bins                                = histogram.bin(6)
      val lastBin                             = bins.last

      val barMax                              = 20
      val maxCount                            = bins.map(_.count).max
      val maxLengthBinLower                   = printDecimal(lastBin.pos).length
      //val maxLengthBinUpper                   = printDecimal(lastBin.max).length

      val stringifyBins = bins
        .map(bin => {
          val binCount         = bin.count
          val binRange         = fillSpaceBefore(printDecimal(bin.pos), maxLengthBinLower)
            //fillSpaceBefore(printDecimal(bin.pos), maxLengthBinUpper))
          //val binStringify = s"""[${binRange._1}, ${binRange._2}["""
          val binStringify = s"""$binRange"""

          if (binCount > 0) {
            val bar = "█" * (binCount * barMax / maxCount).toInt
            s"${printAccumulator(level)}$binStringify |█$bar $binCount"
          } else {
            s"${printAccumulator(level)}$binStringify |█ $binCount"
          }

        })
        .mkString("\n")

/*    s"""|${printAccumulator(level)}$name:
          |$stringifyBins""".stripMargin*/
      printInformation(stringifyBins, name, level, true)
    }

    def printBoth[A](both: Both[A], level: Int, printA: (A, Int) => String) = {
      s"""|${printInformation(printA(both.left, level + 1), "Left", level, true)}
          |${printInformation(printA(both.right, level + 1), "Right", level, true)}""".stripMargin

    }

    def printBothDescribe(describes: Both[Describe], level: Int): String = {
      def printOneDescribe(describe: Describe, level: Int): String = describe match {
        case DescribeLong(value)   => printHistogram(value, level)
        case DescribeDouble(value) => printHistogram(value, level)
        case DescribeString(value) => printHistogram(value, level)
        case DescribeBoolean(nTrue, nFalse) => {
          s"""|${printInformation(nTrue.toString, "Number of false", level)}
              |${printInformation(nFalse.toString, "Number of true", level)}""".stripMargin
        }
        case d: Describe => s"${printAccumulator(level)}Empty describe"
        case _ => printInformation("/!\\ Can not display this describe", "Error", level)
      }

      s"""|${printAccumulator(level)}Describes:
          |${printBoth(describes, level + 1, printOneDescribe)}""".stripMargin
    }

}
