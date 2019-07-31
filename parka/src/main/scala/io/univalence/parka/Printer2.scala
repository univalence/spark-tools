package io.univalence.parka.exp

sealed trait Part
case class Cell(line: String) extends Part
case class Rows(parts: Seq[Part]) extends Part
case class Columns(parts: Seq[Part]) extends Part

case class Configuration()

trait Printer[T] {
  def print(t: T): Part
}

object Printer {

  def instance[T](f: T => Part): Printer[T] = new Printer[T] {
    override def print(t: T): Part = f(t)
  }

  implicit val str: Printer[String] = instance(line => Cell(line))

  type Typeclass[T] = Printer[T]
  import magnolia._

  def combine[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] =
    /*
    Columns(Seq(Rows(caseClass.parameters.map(x => Cell(x.label)),
      Rows(caseClass.parameters.map(x =>    ))))
     */
    ???

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]
}
