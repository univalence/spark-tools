package io.univalence.parka

/**
  * The Constraintor Object is a way to analyse a Parla Result
  * This is convenient if you use Parka to verify that the new version of your project is working as expected
  * For example if your new version should not change anything about the data process then the parka result
  * should be empty. Instead of looking inside the parka result you can just set a constraint about your Parka Result
  * using the Constraintor.
  * Example:
  * Constraintor.respectConstraints(analysis)(isSimilar) should return Pass in case of nothing has changed.
  */
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

  case class colsChange(col: String*) extends Constraint{
    override def ok(pa: ParkaAnalysis): Boolean =
      col.forall(colChange(_).ok(pa))
  }

  case class colNotChange(col: String) extends Constraint{
    override def ok(pa: ParkaAnalysis): Boolean =
      !colChange(col).ok(pa)
  }

  case class colsNotChange(col: String*) extends Constraint{
    override def ok(pa: ParkaAnalysis): Boolean =
      col.forall(colNotChange(_).ok(pa))
  }

  def respectConstraints(pa: ParkaAnalysis)(constraints: Constraint*): Status = {
    val verification = constraints.map(constraint => (constraint, constraint.ok(pa))).filter({case (_, ok) => ok == false})
    verification.isEmpty match{
      case true => Pass
      case false => Fail(verification.map(_._1): _*)
    }
  }
}
