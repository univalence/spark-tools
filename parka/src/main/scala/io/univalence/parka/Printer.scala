package io.univalence.parka

import cats.Monoid
import io.univalence.parka.Part.{ Col, Key, Row, Section, Value }

import scala.collection.immutable
import scala.util.Try

sealed trait Part

object Part {
  case class Section(key: String, element: Part) extends Part
  case class Key(key: String, element: Part) extends Part
  case class Value(string: String) extends Part
  case class Row(element: Part*) extends Part
  case class Col(element: Part*) extends Part

  def toString(element: Part): String = {
    val sep = Printer.sep
    element match {
      case Value(string) => string
      case Section(k, e) => k + ":\n" + toString(e).split('\n').map(sep + _).mkString("\n")
      case Key(k, e) =>
        val se = toString(e)
        if (se.count(_ == '\n') > 1) toString(Section(k, e)) else s"$k: $se"
      case c: Col => c.element.map(toString).mkString("\n")
      case r: Row =>
        val blocks: Seq[Array[String]] = r.element.map(x => toString(x).split('\n'))
        val ws: Seq[Int]               = blocks.map(_.map(_.length).reduceOption(Math.max).getOrElse(0))
        val lines: Int                 = blocks.map(_.length).max

        (0 until lines)
          .map(line => {
            blocks.indices
              .map(col => {
                val v = Try(blocks(col)(line)).getOrElse("")
                v + (" " * (ws(col) - v.length))
              })
              .mkString("  ")
          })
          .mkString("\n")
    }
  }
}

object Printer {
  val sep   = "    "
  val start = ""

  def printAccumulator(level: Int, acc: String = ""): String = start + sep * level + acc

  def printListInformation(info: Part*): Part = Col(info: _*)

  import MonoidGen._

  def printInformation(information: String, field: String, jump: Boolean = false): Part =
    if (jump) Section(field, Value(information)) else Key(field, Value(information))

  def printParkaResult(parkaResult: ParkaResult): Part =
    Section("Parka Result", Part.Col(printInner(parkaResult.inner), printOuter(parkaResult.outer)))

  def printInner(inner: Inner): Part =
    Section(
      "Inner",
      Col(
        printInformation(inner.countRowEqual.toString, "Number of equal row"),
        printInformation(inner.countRowNotEqual.toString, "Number of different row"),
        printDiffByRow(inner.countDiffByRow),
        printInnerByColumn(inner.byColumn)
      )
    )

  def printOuter(outer: Outer, level: Int = 0): Part =
    Section(
      "Outer",
      Col(
        printInformation(outer.countRow.left.toString, "Number of unique row on the left dataset"),
        printInformation(outer.countRow.right.toString, "Number of unique row on the right dataset"),
        printOuterByColumn(bmxToMbx(outer.byColumn))
      )
    )

  def printDiffByRow(differences: Map[Seq[String], Long]): Part =
    Key(
      "Differences by sequence of keys",
      Col(
        differences
          .filter(_._1.nonEmpty)
          .map({
            case (key, value) =>
              Value("Key (" + key.mkString(",").toString + ") has " + value + " occurrence" + {
                if (value > 1) "s" else ""
              })
          })
          .toSeq: _*
      )
    )

  def printMap[T](mp: Map[String, T], printT: T => Part, name: String): Part =
    Section(name, Col(mp.map({ case (k, v) => Key(k, printT(v)) }).toSeq: _*))

  def printInnerByColumn(byColumn: Map[String, Delta]): Part = printMap(byColumn, printDelta, "Delta by key")

  def printOuterByColumn(byColumn: Map[String, Both[Describe]]): Part =
    printMap(byColumn, printBothDescribe, "Describe by key")

  def bmxToMbx[K, T: Monoid](bmx: Both[Map[K, T]]): Map[K, Both[T]] = {
    val mono = implicitly[Monoid[T]]

    import MonoidGen._
    implicitly[Monoid[Map[K, Both[T]]]]
      .combine(bmx.left.mapValues(x => Both(x, mono.empty)), bmx.right.mapValues(x => Both(mono.empty, x)))
  }

  def printDelta(delta: Delta): Part =
    printListInformation(
      printInformation(delta.nEqual.toString, "Number of similarities"),
      printInformation(delta.nNotEqual.toString, "Number of differences"),
      printBothDescribe(delta.describe),
      printDeltaSpecific(delta)
    )

  def printDeltaSpecific(delta: Delta): Part = {
    val error = delta.error

    val m2 = error.counts

    val histograms: immutable.Iterable[Part] = error.histograms.map({
      case (k, v) => printHistogram(v, k)
    })

    def countName(key: String): String = key match {
      case "tf"          => "# true -> false"
      case "ft"          => "# false -> true"
      case "rightToNull" => "# null -> not null"
      case "leftToNull"  => "# not null -> null"
      case _             => "# " + key
    }

    val counts: immutable.Iterable[Part] = error.counts.map({
      case (k, v) => printInformation(v.toString, countName(k))
    })

    printListInformation((counts ++ histograms).toSeq: _*)
  }

  def printHistogram(histogram: Histogram, name: String = "Histogram"): Part = {
    val bins                                = histogram.bin(6)
    def printDecimal(value: Double): String = f"$value%.2f"
    val histobar                            = "o"
    val barMax                              = 22
    val maxCount                            = bins.map(_.count).max
    val maxLengthBinLower                   = bins.map(bin => printDecimal(bin.pos).length).max

    def fillSpaceBefore(value: String, focus: Int = 0): String = " " * Math.max(focus - value.length, 0) + value

    Section(
      name,
      Col(bins.map(b => {
        val barL = (b.count.toDouble / maxCount * barMax).toInt
        Value(printDecimal(b.pos) + " | " + (histobar * barL) + (" " * (barMax - barL)) + " " + b.count)
      }): _*)
    )
  }

  def printBoth[A](both: Both[A], printA: A => Part): Part =
    Row(Section("Left", printA(both.left)), Section("Right", printA(both.right)))

  def printOneDescribe(describe: Describe): Part = {
    import describe._

    val strHistogram: Seq[Part] = histograms.keys.map(k => printHistogram(histograms(k))).toSeq

    def keyTitle(key: String): String = key match {
      case "nTrue"  => "Number of true"
      case "nFalse" => "Number of false"
      case "nNull"  => "Number of null"
      case _        => s"# $key"
    }

    val strCounts: Seq[Part] = counts
      .map({
        case (k, v) => printInformation(v.toString, keyTitle(k))
      })
      .toSeq

    printListInformation(strCounts ++ strHistogram: _*)
  }

  def printBothDescribe(describes: Both[Describe]): Part = Section("Describes", printBoth(describes, printOneDescribe))
}
