package io.univalence.fenek.analysis

import io.univalence.fenek.Expr
import io.univalence.fenek.Expr.UntypedExpr
import io.univalence.fenek.generic.GenericExpr
import io.univalence.typedpath.Key

object Sources {

  //TODO Test and redo
  def getSources(expr: UntypedExpr): Vector[Key] =
    /*
    def loop(genericExpr: GenericExpr, suffix: Vector[String] = Vector.empty): Vector[Path] =
      genericExpr.expr.value match {
        case Expr.Ops.SelectField(name, source) =>
          loop(GenericExpr(source), name +: suffix)
        case Expr.Ops.RootField(name) => Vector(Path(name +: suffix))
        case _ =>
          for {
            sourceline <- genericExpr.sources.toVector
            source     <- sourceline.value
            x          <- loop(source)
          } yield x

      }

    loop(GenericExpr(expr))*/
    ???
}
