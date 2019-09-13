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

  def list(info: Part*): Part = Col(info: _*)

  import MonoidGen._

  def information(field: String, information: String, jump: Boolean = false): Part =
    if (jump) Section(field, Value(information)) else Key(field, Value(information))

  def enum(data: Map[EnumKey, Long]): Part = {
    Col(data.map({case (k, v) =>  information(k.toString, v.toString)}).toSeq: _*)
  }

  def printParkaResult(parkaResult: ParkaResult): Part =
    Section("Parka Result", Part.Col(printInner(parkaResult.inner), printOuter(parkaResult.outer)))

  def printInner(inner: Inner): Part =
    Section(
      "Inner",
      Col(
        information("Number of equal row", inner.countRowEqual.toString),
        information("Number of different row", inner.countRowNotEqual.toString),
        printDiffByRow(inner.countDeltaByRow.mapValues(_.count)),
        map(inner.byColumn, "Delta by key")(x => x, delta)
      )
    )

  def printOuter(outer: Outer): Part =
    Section(
      "Outer",
      Col(
        information("Number of unique row on the left dataset", outer.both.left.count.toString),
        information("Number of unique row on the right dataset", outer.both.right.count.toString),
        printOuterByColumn(bothMap2MapBoth(outer.both.map(_.byColumn)))
      )
    )

  def printOuterByColumn(byColumn: Map[String, Both[Describe]]): Part =
    map(byColumn, "Describe by key")(x => x, bd => Section("Describes", printBoth(bd, printOneDescribe)))

  def printDiffByRow(differences: Map[Set[String], Long]): Part =
    Key(
      "Differences by sequence of keys",
      Col(
        differences
          .map({
            case (key, value) =>
              Value("Key (" + key.mkString(",").toString + ") has " + value + " occurrence" + {
                if (value > 1) "s" else ""
              })
          })
          .toSeq: _*
      )
    )

  def map[K, T](mp: Map[K, T], name: String)(keyT: K => String, valueT: T => Part) =
    Section(name, Col(mp.map({ case (k, v) => Key(keyT(k), valueT(v)) }).toSeq: _*))

  def printInnerByColumn(byColumn: Map[String, Delta]): Part = map(byColumn, "Delta by column")(x => x, delta)

  //def printOuterByColumn(byColumn: Map[String, Both[Describe]]): Part = map(byColumn, "Describe by key")(x => x,x => printBoth(x, printOneDescribe))

  def bothMap2MapBoth[K, T: Monoid](bmx: Both[Map[K, T]]): Map[K, Both[T]] = {
    val mono = implicitly[Monoid[T]]

    import MonoidGen._
    implicitly[Monoid[Map[K, Both[T]]]]
      .combine(bmx.left.mapValues(x => Both(x, mono.empty)), bmx.right.mapValues(x => Both(mono.empty, x)))
  }

  def delta(delta: Delta): Part =
    list(
      information("Number of similarities", delta.nEqual.toString),
      information("Number of differences", delta.nNotEqual.toString),
      Section("Describes", printBoth(delta.describe, printOneDescribe)),
      printDeltaSpecific(delta)
    )

  def printDeltaSpecific(delta: Delta): Part = {
    val error = delta.error

    val strHistogram: immutable.Iterable[Part] = error.histograms.map({
      case (k, v) => printHistogram(v, "Delta")
    })

    def countName(key: String): String = key match {
      case "tf"          => "# true -> false"
      case "ft"          => "# false -> true"
      case "rightToNull" => "# null -> not null"
      case "leftToNull"  => "# not null -> null"
      case _             => "# " + key
    }

    val strCounts: immutable.Iterable[Part] = error.counts.map({
      case (k, v) => information(countName(k), v.toString)
    })

    val strEnums: immutable.Iterable[Part] = error.enums.map({
      case (k, v) => Section(k, enum(v.heavyHitters))
    })

    list((strCounts ++ strHistogram ++ strEnums).toSeq: _*)
  }

  def printHistogram(histogram: Histogram, name: String = "Histogram"): Part = {
    val bins                                = histogram.bin(6)
    def printDecimal(value: Double): String = f"$value%.2f"
    def fillSpaceBefore(value: String, focus: Int = 0): String = " " * Math.max(focus - value.length, 0) + value
    val histobar          = "o"
    val barMax            = 22
    val maxCount          = bins.map(_.count).max
    val maxLengthBinLower = bins.map(bin => printDecimal(bin.pos).length).max

    Section(
      name,
      Col(bins.map(b => {
        val barL = (b.count.toDouble / maxCount * barMax).toInt
        Value(
          fillSpaceBefore(printDecimal(b.pos), maxLengthBinLower) + " | " + (histobar * barL) + (" " * (barMax - barL)) + " " + b.count
        )
      }): _*)
    )
  }

  def printBoth[A](both: Both[A], printA: A => Part): Part =
    Row(Section("Left", printA(both.left)), Section("Right", printA(both.right)))

  def printOneDescribe(describe: Describe): Part = {
    import describe._

    val strHistogram: Seq[Part] = histograms.keys.map(k => printHistogram(histograms(k), "Values repartition")).toSeq

    def keyTitle(key: String): String = key match {
      case "nTrue"  => "Number of true"
      case "nFalse" => "Number of false"
      case "nNull"  => "Number of null"
      case _        => s"# $key"
    }

    val strCounts: Seq[Part] = counts
      .map({
        case (k, v) => information(keyTitle(k), v.toString)
      })
      .toSeq

    val strEnums: immutable.Iterable[Part] = enums.map({
      case (k, v) => Section(k, enum(v.heavyHitters))
    })

    list(strCounts ++ strHistogram ++ strEnums: _*)
  }

}
