package io.univalence.parka

object Constraintor {
  sealed trait Status
  case object Pass extends Status
  case class Fail(constraints: Constraint*) extends Status

  sealed trait Constraint{
    def ok(pa: ParkaAnalysis): Boolean
  }

  case object isSimilar extends Constraint {
    override def ok(pa: ParkaAnalysis): Boolean =
      noOuter.ok(pa) && noInner.ok(pa)
  }
  case object noOuter extends Constraint{
    override def ok(pa: ParkaAnalysis): Boolean =
      pa.result.outer == Outer(Both(DescribeByRow(0, RowBasedMap.empty),DescribeByRow(0, RowBasedMap.empty)))
  }
  case object noInner extends Constraint{
    override def ok(pa: ParkaAnalysis): Boolean =
      pa.result.inner.countRowNotEqual == 0
  }

  case class colChange(col: String) extends Constraint{
    override def ok(pa: ParkaAnalysis): Boolean =
      pa.result.inner.byColumn.get(col).map(!_.error.equals(Describe.empty)).getOrElse(throw new Exception("Fail"))
  }

  case class colNotChange(col: String) extends Constraint{
    override def ok(pa: ParkaAnalysis): Boolean =
      !colChange(col).ok(pa)
  }

  def respectConstraints(pa: ParkaAnalysis)(constraints: Constraint*): Status = {
    val verification = constraints.map(constraint => (constraint, constraint.ok(pa))).filter({case (_, ok) => ok == false})
    verification.isEmpty match{
      case true => Pass
      case false => Fail(verification.map(_._1): _*)
    }
  }

}
